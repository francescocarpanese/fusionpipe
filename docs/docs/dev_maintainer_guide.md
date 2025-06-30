This is the developer guide for `fusionpipe`.

`fusionpipe` is lightwise pipeline orchestrator to help organise data-analysis, simulation, machine learning pipelines. It is composed by the following components:

- Frontend in Svelte. 
- Backend in python. 
- FASTAPI web-app for communication between frontend and backend.
- The backend streams information to a postgresSQL database

In this document you will find in order:

- Installation of the pre-requisite.
- Local installation for single user. This u
- Local installation for multiple users which have access to the machine.

# Pre-requiste

## Postgres
PostgreSQL is used in order to keep track of the pipeline database. As a developer there are ways in which you can set it up:
- Set-up postgres in your local machine.
- Use docker to set up postgres.

It is recommended to use docker, as it will allow you to have the same environment in dev and prod.


### local installation

- Install postgres

- Initialise postgtes
`sudo /usr/bin/postgresql-setup --initdb`

Usually postgres is initialised creating the admin postgres user called `postgres`.

The default set-up for postgres to check the name of the user, to grant access to the database (`peer` option). 
In this condition, in order to perform admin operation on the databaset, you need to log as `postgres` user (requires sudo access).

It is possibile to change the postgres authentication defualt mechanism, changing the file `pg_hba.conf` usually found in the location
`/var/lib/pgsql/data/pg_hba.conf`. Changing `peer` to `md5` will ask for password authentication at the login.


### docker installation

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


# Set-up database

- Create a new database `fusionpipe_prod1`.
```bash
sudo -u postgres createdb fusionpipe_prod1
```

- Create the role `fusionpipeusers` which will allow to read and write to `
```bash
psql -U postgres -c "CREATE ROLE fusionpipeusers;"
```

- Allow users to in role to connect to databaset
```bash
psql -U postgres -d fusionpipe_prod1 -c "GRANT CONNECT ON DATABASE fusionpipe_prod1 TO fusionpipeusers;"
```

- Grant all provilidges to user on datbase `fusionpipe_prod1`.
```bash
psql -U postgres -d fusionpipe_prod1 -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fusionpipeusers;"
psql -U postgres -d fusionpipe_prod1 -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO fusionpipeusers;"
```

- Add `fusionpipeadmin` to the group.
```bash
psql -U postgres -d fusionpipe_prod1 -c "GRANT fusionpipeusers TO fusionpipeadmin;"
```

# Create and initialise the required tables for the database

- If you are working with a dev database, initialise the databse or clear the databse with the script.
Before running the script make sure your environment variables are pointing to the right dev database.
```bash
uv run python src/fusionpipe/utils/init_database.py
```

```bash
set -a
source production.env
set +a
```

- Start the backend
Making sure you are in a terminal where you have the right enviroment path.


- Start the frontend

```bash
VITE_BACKEND_HOST=$VITE_BACKEND_HOST VITE_BACKEND_PORT=$VITE_BACKEND_PORT  npm run dev -- --port $VITE_FRONTEND_PORT
```


# R/W access to folder

- Create the folder that will contain applicaiton data

- Create a group
```bash
sudo groupadd fusionpipeusers
```

- Log out and log in again

- Add user to the group
```bash
sudo usermod -aG fusionpipeusers carpanes
```

- Log out and log in every time you add a new user.

- Set group permission permission to the shared data folder
```bash
sudo chown -R :fusionpipeusers /misc/fusionpipe_shared
sudo chmod -R 2770 /misc/fusionpipe_shared
```

- (Optional) Enforce Permissions with ACLs. This will let other user write on user generated file, independently of the user mask
```bash
sudo setfacl -d -m g::rwx /misc/fusionpipe_shared
sudo setfacl -d -m o::--- /misc/fusionpipe_shared
```

TODO grant reading access to the user utils folder.


### Onboard new user

- Create new user with `<newusername>` in postgres 

`psql -U postgres -d fusionpipe_prod1 -c "CREATE USER <newusername>;"`

- Grant user access to role in postgres database

`psql -U postgres -d fusionpipe_prod1 -c "GRANT fusionpipeusers TO <newusername>;"`

- Ask user to write the following line in the `.profile` or `.bashrc_profile`, in order to persist the access to the database

```bash
export DATABASE_URL="dbname=fusionpipe_prod1 port=5432"
```

- Write the location of the user utils in the `.profile`, `.bashrc_profile`.
```bash
export USER_UTILS_FOLDER_PATH="/home/fusionpipeadmin/Documents/fusionpipe/src/fusionpipe/user_utils"
```

- Add group user to the group in the server, in order for him to have access to the shared repository
```bash
sudo usermod -aG fusionpipeusers username
```

### User set-up
- Communicate the username to the admin
```bash
whoami
```

- Install `uv`
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

- ssh to the machine and forward the port with the frontend and backend

```bash
ssh -L 5176:localhost:5176 -L 8101:localhost:8101 <username>@spcdefuse01
```

- Write the following line in the `.profile`. This allows the user to have access to the database
```bash
export DATABASE_URL="dbname=fusionpipe_prod1 port=5432"
```

- Write the location of the user utils in the `.profile`
```bash
export USER_UTILS_FOLDER_PATH="/misc/carpanes/fusionpipe/src/fusionpipe/user_utils"
```

- When switching to a node run the following command
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
