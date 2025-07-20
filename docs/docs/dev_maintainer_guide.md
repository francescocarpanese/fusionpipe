This is the developer/maintainer guide for `fusionpipe`.

`fusionpipe` is lightwise pipeline orchestrator to help organise data-analysis, simulation, machine learning pipelines. It is composed by the following components:

- Frontend in Svelte. 
- FASTAPI web-app as backend.
- A postgresSQL database which is used 

With the current guidelines `fusionpipe` 
This guide has different level of complexity depending on what is your role an your deployment solution:

- **user** (without installation): In case you are only using `fusionpipe` which is already set-up by your IT, refer to the user guide
- **user** (with installation): In case you would like to run `fusionpipe` locally on your device and use it. This is the same set-up as for the maintainer. If you are the only user, the installation is simplified.
- **developer**: In this case you would like to contribute to the development of `fusionpipe`. You will have some special command to run the different systems in debugging mode. It is expected that in this set-up you will be developing the application as a single user.
- **maintainer**: If you are a maintainer of `fusionpipe`, you are deploying fusion for yourself and potentially other users. In case you will have multiple users using the service, you will need some extra set-up to grant the necessary permission to the different users. 

Some operation during the installation process will require to have sudo access. Reach out to your admin if needed. The operation that requires sudo are:

- Setting up `postgresSQL` 
- Grant access to `docker` group to your user.
- Extend permission r/w permission if setting up the deployment for multiple users.
- Networking in case you are deploying the solution on a server which firewall and port restriction.

In summary `fusionpipe` has the following things to be set-up:

- Svelte application
- FAST-API backend
- PostgresSQL database

In case of multiple users installation, also the following needs to be set-up.

- Shared access folder for the data.
- Shared access folder for the `uv` caching folder environment.
- Shared access for the user API utilities folder.

## Prerequistes

### Postgres
[PostgreSQL](https://www.postgresql.org/) is used as a database to keep track of the relation between nodes and pipeline and other metadata.
- Set-up postgres in your local machine.
- Use docker to set up postgres.

#### local installation (need sudo)

- Install postgres

- Initialise postgtes
`sudo /usr/bin/postgresql-setup --initdb`

Usually postgres is initialised creating the OS user `postgres`, which serves as admin the database operation.

The default set-up for postgres to check the OS user, to grant access to the database (`peer` option). 
In this condition, in order to perform admin operation on the database, you need to log as `postgres` user (requires sudo access).

It is possibile to change the postgres authentication defualt, changing the file `pg_hba.conf` usually found in the location
`/var/lib/pgsql/data/pg_hba.conf`. Changing `peer` to `md5` will ask for password authentication at the login instead of 

After changing the authorisatio, reload postgres
```bash
sudo systemctl reload postgresql
```

#### docker installation 
* TO BE UPDATED *

Set the env variable.

- Modify the variables into the `developer.env` file.

- Navigate the docker folder
```bash
cd docker
```

- Set up the variables in `docker-compose-psg.yml`. See documentation in the file.
- If you already have a database and you want to remove all the data related to it, delete the folder with the database location. Otherwise, to continue with the existing database, skip this step.
- Run the docker compose command to start the postgres database.
```bash
docker-compose -f docker-compose-psg.yml up
```
This will startup yuor postgres database in a docker container, and expose to the port specified in the `docker-compose-psg.yml` file.

Test connection
```bash
psql -h localhost -p 5542 -U <user> -d <database>
```

### UV package manager
UV package manager is the solution used to in order to deal with python packages.


## Set-up the database
The following commands needs to be run from the OS user `postgres` which has all the access.
It is assumed that there is a priviledge OS user, which is the maintainer/developer for `fusionpipe` in your machine.
There are different possibilities for that.

- If you are the only user/developer, use your user as admin.
- If you are the maintainer for fusionpipe on a server, we recommend to create an OS user which will manage the `fusionpipe` installation. We will assume this user to be to be called `fusionpipeadmin` in the following.


The following opeartion must be done in the order they are presented

- Log in as `postgres` user in your machine
```bash
sudo -u postgres -i
```

- Create the database
```bash
createdb fusionpipe_prod1
```

- Check that the database has been created
```bash
psql -U postgres -l
```

- Create a ROLE which will be able to modify the database. User will be added to this role
```bash
psql -c "CREATE ROLE fusionpipeusers NOLOGIN;"
```

- Grant connection to the role `fusionpipeusers`
```bash
psql -U postgres -d fusionpipe_prod1 -c "GRANT CONNECT ON DATABASE fusionpipe_prod1 TO fusionpipeusers;"
```

- Grant usage to the role `fusionpipeusers`
```bash
psql -d fusionpipe_prod1 -c "GRANT USAGE ON SCHEMA public TO fusionpipeusers;"
```

- As we want only the admin user to have the possibility to create table in the database, revoke some access access to other users.
```bash
psql -d fusionpipe_prod1 -c "REVOKE CREATE ON SCHEMA public FROM PUBLIC;"
```
```bash
psql -d fusionpipe_prod1 -c "REVOKE CREATE ON SCHEMA public FROM fusionpipeusers;"
```

- Grant the possibility to the ROLE `fusionusers` to be modify the table in the database.
```bash
psql -d fusionpipe_prod1 -c "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fusionpipeusers;"
```

- Grant the possibiliy to create tables to the `fusionpipeadmin`.
```bash
psql -d fusionpipe_prod1 -c "GRANT CREATE ON SCHEMA public TO fusionpipeadmin;"
```

- Extend the priviledges of `fusionpipeusers` to `fusionpipeadmin`.
When you add `fusionpipeadmin` to the `fusionpipeusers` role in PostgreSQL:

```bash
psql -U postgres -d fusionpipe_prod1 -c "GRANT fusionpipeusers TO fusionpipeadmin;"
```

This **extends** the permissions of `fusionpipeadmin` to include all privileges granted to `fusionpipeusers`. The `fusionpipeadmin` will retain its own privileges and additionally inherit those of `fusionpipeusers`. It does **not** restrict `fusionpipeadmin` to only the permissions of `fusionpipeusers`; instead, it is a superset of both.


- The following command updates the default privileges for tables created in the `public` schema of the `fusionpipe_prod1` database. It ensures that any new tables will automatically grant `SELECT`, `INSERT`, `UPDATE`, and `DELETE` permissions to the `fusionpipeusers` role. Run this command as the `fusionpipeadmin` user:
```bash
psql -U fusionpipeadmin -d fusionpipe_prod1 -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fusionpipeusers;"
```


# Create and initialise the required tables for database

> **⚠️ WARNING:** The following operations will modify your PostgreSQL database. Ensure you have backed up any important data before proceeding. Only users with the appropriate privileges should perform these actions.

The database needs to be initialised with the tables for the `fusionpipe` application. Given the role that have been granted in the previous step, only the `fusionpipeadmin` user is able to create this.

- Add to the `.bash_profile` of `fusionpipeadmin` user. This will allow the `fusionpipeuser` to connect to the database through `peer` connection. (See postgres SQL documentation).
```bash
export DATABASE_URL="dbname=fusionpipe_prod1 port=5432"
```

- Initialise the tables in the database. This is conveniently done runnig the script. 
```bash
uv run python fusionpipe/src/fusionpipe/utils/init_database.py
```

# Create data folder and extend R/W access for multiple users
- Create the folder that will contain application data for `fusionpipe` data application. 
```bash
mkdir <fusion_pipe_data_folder>
```

The following steps are only needed if you are setting-up `fusionpipe` for multiple users to have access to your machine.
If you are single developer, or single user you can skip them.

- Create a group. This way you will be able to control the user access to data based on the group.
```bash
sudo groupadd fusionpipeusers
```

- Log out and log in again to allow the propagation of the new group.

- Add `fusionpipeadmin` admin user to the group
```bash
sudo usermod -aG fusionpipeusers fusionpipeadmin
```
- Log out and log in every time you add a new user.

- Set group permission permission to the shared data folder
```bash
sudo chown -R :fusionpipeusers <fusion_pipe_data_folder>
sudo chmod -R 2770 <fusion_pipe_data_folder>
```

- Enforce Permissions with ACLs. This will let other user to write on files generated by other users, independently of the user mask.
```bash
sudo setfacl -d -m g::rwx /misc/fusionpipe_shared
sudo setfacl -d -m o::--- /misc/fusionpipe_shared
```
The second special folder is the one containing the user utility scripts. This needs to be (only) readable by all users of `fusionpipe`.

- Login with `fusionpipeadmin` in the folder containing the source code of fusion pipe.
- Grant reading access to the group `fusionpipeusers`
```bash
sudo chown -R :fusionpipeusers fusionpipe/src/fusionpipe/user_utils
sudo chmod -R 2750 fusionpipe/src/fusionpipe/user_utils
```

# Set enviroment variables
If you are the maintainer or single developer user, we recommend to collect all your environment variables into a single file `<myenvfile>.env`.
Careful with the syntax, `.env` file does not allow for spaces in the definition definition of the environment variables. The strings needs to be withing `""` and not `''`. 
The full set of environment variables needed to run all services of `fusionpipe` are:

```bash
FUSIONPIPE_DATA_PATH="<absolute/path/to/fusionpipe/data/folder>"
UV_CACHE_DIR="<absolute/path/to/uv/cache/dir>"
DATABASE_URL="dbname=<database_name> port=<postgres_port>"
BACKEND_HOST="localhost"
BACKEND_PORT=<backend_port>
VITE_BACKEND_PORT=${BACKEND_PORT}
VITE_BACKEND_HOST=${BACKEND_HOST}
VITE_FRONTEND_PORT=<frontend_port>
VITE_FRONTEND_HOST="localhost"
USER_UTILS_FOLDER_PATH="<absolute/path/to/user/utils>"
FP_MATLAB_RUNNER_PATH="<abosolute/path/to/matlab/executable>"
VIRTUAL_ENV="<(optional) default virtual env to activate when running the backend>"
DATABASE_URL_TEST="dbname=<database_test_name> port=<postgres_port>"
```

In the following some explanation of the different environment variables: 

- `<absolute/path/to/fusionpipe/data/folder>`: Data from `fusionpipe` will be stored in this folder.
- `<absolute/path/to/uv/cache/dir>`: This will contain the cache for the uv environment.
- `<database_name>`: The database name that you have created in previous steps.
- `<postgres_port>`: The port where postgresSQL is avilable in your host. Defaulat 5432
- `<backend_port>`: Port for the FAST-API backend. We recommend to user a port >8000
- `<frontend_port>`: This is the port for Svelte frontent. Usually Svelte is using a port >5000
- `<absolute/path/to/user/utils>`: This is the absolute path where the user utilities, which needs to be readable by all users, are stored.
- `<abosolute/path/to/matlab/executable>`: It is conveninet to set-up the path to your local installation of matlab if you are considering using it for development.
- `<database_test_name>`: The database name used for the test suite. Usually `fusionpipe_test`.

# Run the frontend and backend (developer mode)

- Open a new terminal

- Set the environment variable for the terminal. You can set all the variables from your env file with the command.
```bash
set -a
source <myenvfile.env>
set +a
```

- Navigate the backend folder
```bash
cd fusionpipe/src/fusionpipe/api
```

- Start the backend service
```bash
uv run python main.py
```

- Open a new terminal 

- Set the environment variable as above also in this terminal

- Navigate the frontend folder
```bash
cd fusionpipe/src/fusionpipe/frontend
```

- Start the Svelte application in debug mode
```bash
VITE_BACKEND_HOST=$VITE_BACKEND_HOST VITE_BACKEND_PORT=$VITE_BACKEND_PORT  npm run dev -- --port $VITE_FRONTEND_PORT
```

- Open the browser at the port where the frontend is served. For example
```bash
localhost:5174
```

If your are developing the application and your environment variables are not change, you can consider to add them in your `.bashrc` or `.bashrc_profile` in order to have them loaded directly when you log with your user.

## Compile the frontend and serve
If you want to compile the frontend 


You need to set the environment variable before building the app as the environemtn variable will be backed in the app
```bash
npm run build
```

```bash
npm run preview -- --port $VITE_FRONTEND_PORT
```

```bash
npx serve -s dist -l $VITE_FRONTEND_PORT
```

- Start ray cluster locally if you want to use it for paralellelisation
```bash
ray start --head
```


# Run the frontend and backend (production)

## Run fusionpipe in as systemd (Unstable)

- Save the enviroment variable file in location, hidden from the user and protect it.

- Create the user systemd service directory (if it doesn’t exist):
```bash
mkdir -p ~/.config/systemd/user
```

- Write `systemd` configuration file `~/.config/systemd/user/fusionpipe_backend.service`
```bash
[Unit]
Description=Run the backend

[Service]
Type=simple
WorkingDirectory=/home/fusionpipeadmin/Documents/fusionpipe
EnvironmentFile=<absolute_path_to_env_file>
Environment="PATH=/home/fusionpipeadmin/.local/bin:%h/.local/bin:/usr/bin:/bin"
ExecStart=/home/fusionpipeadmin/.local/bin/uv run uvicorn fusionpipe.api.main:app --host 0.0.0.0 --port 8101
Restart=on-failure

[Install]
WantedBy=default.target
```

!!! Warning
    The extra `Environment` in the previous service is done to prepend the bin path to the system d. Otherwise this can result in not seeing the `uv` installation, making the nodes fail to run.


- Write `systemd` configuration file `~/.config/systemd/user/fusionpipe_frontend.service`
```bash
[Unit]
Description=Run fusionpipe frontend

[Service]
Type=simple
EnvironmentFile=<absolute_path_to_env_file>
WorkingDirectory=/home/fusionpipeadmin/Documents/fusionpipe/src/fusionpipe/frontend
ExecStart=npx serve -s dist -l $VITE_FRONTEND_PORT
Restart=on-failure

[Install]
WantedBy=default.target
```

Ray clustet can be set up to orchestrate parallel workflow.

- Set up folder for ray cluster temporary file. This must be inside the folder shared with other users.
```bash
mkdir -p <data_temporary_file>
```

- Write `systemd` configuration file `~/.config/systemd/user/fusionpipe_ray.service`
```bash
[Unit]
Description=Ray Cluster Service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/fusionpipeadmin/Documents/fusionpipe
EnvironmentFile=<absolute_path_to_env_file>
Environment="PATH=/home/fusionpipeadmin/.local/bin:%h/.local/bin:/usr/bin:/bin"
ExecStart=/home/fusionpipeadmin/.local/bin/uv run ray start --head --temp-dir <path_to_tmp_folder_in_shared_folder>
ExecStop=/home/fusionpipeadmin/.local/bin/uv run ray stop
TimeoutStopSec=30

[Install]
WantedBy=default.target
```

- Reload the deamon
```bash
systemctl --user daemon-reload
```

- Enable and start the backend
```bash
systemctl --user enable fusionpipe_backend.service
systemctl --user start fusionpipe_backend.service
systemctl --user status fusionpipe_backend.service
```

- Enable and start the frontend
```bash
systemctl --user enable fusionpipe_frontend.service
systemctl --user start fusionpipe_frontend.service
systemctl --user status fusionpipe_frontend.service
```

- Enable and start ray cluster
```bash
systemctl --user enable fusionpipe_ray.service
systemctl --user start fusionpipe_ray.service
systemctl --user status fusionpipe_ray.service
```

- Let the user systemd be run when the user disconnect
```bash
sudo loginctl enable-linger $USER
```

- Stop services
```bash
systemctl --user stop fusionpipe_frontend.service
systemctl --user stop fusionpipe_backend.service
systemctl --user stop fusionpipe_ray.service
```

- kill processes if they don't stop by themselves.
```bash
systemctl --user kill fusionpipe_frontend.service
```

- Restart the services
```bash
systemctl --user restart fusionpipe_backend.service
systemctl --user restart fusionpipe_frontend.service
systemctl --user restart fusionpipe_ray.service
```

# Upgrade the production version
- Login with the `fusionpipe
- Navigate the `fusionpipeadmin` user.
- Pull the desired version of fusionpipe
```
git pull
```
- Stop services
```bash
systemctl --user stop fusionpipe_frontend.service
systemctl --user stop fusionpipe_backend.service
```

- Navigate the frontend folder
```bash
cd src/fusionpipe/frontend
```

- Set the environment variable from from your production env. See section above

- Recompile the frontend
```bash
npm run build
```

- Start the systemd services
``` bash
systemctl --user start fusionpipe_backend.service
systemctl --user start fusionpipe_frontend.service
```

- Check that the services are working properly
```bash
systemctl --user status fusionpipe_backend.service
systemctl --user status fusionpipe_frontend.service
systemctl --user status fusionpipe_ray.service
```

# Onboard a new user

- Create new user with `<newusername>` in postgres 

`psql -U postgres -d fusionpipe_prod1 -c "CREATE USER <newusername>;"`

- Grant user access to role `fusionpipeusers` in postgres database

`psql -U postgres -d fusionpipe_prod1 -c "GRANT fusionpipeusers TO <newusername>;"`

- Ask user to write the following line in the `.profile` or `.bashrc_profile`, in order to persist the access to the database when it log in.

```bash
export DATABASE_URL="dbname=fusionpipe_prod1 port=5432"
```

- Write the location of the user utils in the `.profile`, `.bashrc_profile`.
```bash
export USER_UTILS_FOLDER_PATH="<absolute/path/to/user/utils>"
```

- Add user to the group in the server, in order for him to have access to the shared repository
```bash
sudo usermod -aG fusionpipeusers username
```

# User set-up
- Communicate the username to the admin
```bash
whoami
```

- Install `uv`
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

- ssh to the machine and forward the port with the frontend and backend. Ask admin to tell the port used for deployment. 
```bash
ssh -L <fronend_port>:localhost:<fronend_port> -L <backend_port>:localhost:<backend_port> -L <ray_port>:localhost:<ray_port>  <username>@<host> 
```
Usually ray port is set to 8265

- Write the following line in the `.profile`. This allows the user to have access to the database
```bash
export DATABASE_URL="dbname=fusionpipe_prod1 port=5432"
```

- Write the location of the user utils in the `.profile`
```bash
export USER_UTILS_FOLDER_PATH="<absolute_path_to_user_utils>"
```

- When switching to a node run, if you want to user jupyter notebooks, run the following command to initialise the python kernel.
```bash
uv run python init_node_kernel.py
```

# Set up docker at system level. (require sudo)
Or ask your admin to do that for you.

```bash
sudo apt-get update
sudo apt-get install docker.io
```

Start docker service at system level
```bash
sudo systemctl enable docker
sudo systemctl start docker
```

Add your user to docker
```bash
sudo usermod -aG docker $USER
```

Re-start your user session, and check your user is in the docker group
```bash
groups
```

Check if docker compose is installed
```bash
docker compose version
```

Otherwise install it with
```bash
pip install docker-compose
```

Docker can load many images, that can occopy space in your disk. You might want to consider to set the location where docker images are saved.
