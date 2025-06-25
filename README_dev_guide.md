
# Set up docker. You need sudo access
Or ask your admin to do that for you.

```bash
sudo zypper refresh
sudo zypper install docker

sudo systemctl enable docker
sudo systemctl start docker
```

Add your user to docker

```
sudo usermod -aG docker $USER
```

Re-start your user session, and check your user is in the docker group
```bash
groups
```

Install docker compose

```bash
docker compose version
```

If you have python
```
pip install docker-compose
```

otherwise load the binaries.

Docker can load many images, that can occopy space in your disk. You might want to consider to set the location where docker images are saved.

Look for "how to change the docker data directory"


# Postgres
PostgreSQL is used in order to keep track of the pipeline database. As a developer there are ways in which you can set it up:
- Set-up postgres in your local machine.
- Use docker to set up postgres.

It is recommended to use docker, as it will allow you to have the same environment in dev and prod.

## Set up postgres with docker

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

- If you are working with a dev database, initialise the databse or clear the databse with the script.
Before running the script make sure your environment variables are pointing to the right dev database.
```bash
uv run python src/fusionpipe/utils/init_database.py
```

# Set up with local installation of postgres
- Seek documentation to install postgres locally.


# Set-up production server

- Create the database

```bash
psql -U postgres -d fusionpipe_prod1 -c "CREATE ROLE writers;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT CONNECT, TEMPORARY ON DATABASE fusionpipe_prod1 TO writers;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO writers;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT CREATE ON SCHEMA public TO writers;"
psql -U postgres -d fusionpipe_prod1 -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO writers;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO writers;"
```

- Grant access to the user.

```bash
psql -U postgres -d fusionpipe_prod1 -c "GRANT writers TO carpanes;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT writers TO fbertini;"
```



- Export the env variable
`export $(grep -v '^#' something.env | xargs)`

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

# Set up a new user 

- Create new user in postgres

`psql -U postgres -d fusionpipe_prod1 -c "CREATE USER fbertini WITH PASSWORD 'coccolone';"`

- Grant user access to group

`psql -U postgres -d fusionpipe_prod1 -c "GRANT writers TO fbertini;"`

- Ask user to write the following line in the `.bashrc`, in order to persist the access to the database

```bash
export DATABASE_URL="dbname=fusionpipe_prod1 user=carpanes password=zidane90 host=localhost port=5432"
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

- ssh to the machine and forward the port with the frontend and backend

```bash
ssh -L 5174:localhost:5174 -L 8100:localhost:8100 fbertini@spcpc636
```

- Write the following line in the `.profile`. This allows the user to have access to the database
```bash
export DATABASE_URL="dbname=fusionpipe_prod1 user=fbertini password=coccolone host=localhost port=5432"
```

- When switching to a node run the following command
```bash
uv run python -m ipykernel install --user --name n_20250624150209_5920 --display-name n_20250624150209_5920
```
To initialise the kernel for the node


# Make the noteboo available to everybody
# (This is not working yet)
sudo groupadd jupyterkernels
sudo mkdir -p /usr/local/share/jupyter/kernels
sudo chown -R root:jupyterkernels /usr/local/share/jupyter/kernels
sudo chmod -R 2775 /usr/local/share/jupyter/kernels
sudo usermod -aG jupyterkernels username