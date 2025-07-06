This guideline will guide you through on how to install `fusionpipe` for a local installation in case you are a single user or developer. 

`fusionpipe` is lightwise pipeline orchestrator to help organise data-analysis, simulation, machine learning pipelines. It is composed by the following components:

- Frontend develop in [Svelte](https://svelte.dev/).
- [FASTAPI](https://fastapi.tiangolo.com/) web-app as backend.
- A [postgresSQL](https://www.postgresql.org/) database which is used to keep track of the relations between nodes and pipelines.

!!! warning 
    It assumes that you have `sudo` access to the machine, or that you can contact your IT admininstator to perform the necessary setting for postgresSQL. See the following for more information 

In the current status the installation guideline is provided for a Bare Metal unix system. In figure a dockerised version on the installation is under development.

## Prerequisites

### Set up postgresSQL

Install posgresSQL following the guidelines in the official [posgresSQ](https://www.postgresql.org/download/) website.

- Login with the `postgres` user to provide necessary access to the database for your personal user
```bash
sudo -u postgres -i
```

- Create a database
```bash
createdb fusionpipe_dev
```

- Grant permission to your OS user, `<myosuser>` to create schema in the database and modify entries 

```bash
psql -U postgres -c "CREATE ROLE <myosuser>;"

psql -U postgres -d fusionpipe_dev -c "GRANT CONNECT ON DATABASE fusionpipe_dev TO <myosuser>;"

psql -U postgres -d fusionpipe_dev -c "GRANT ALL ON SCHEMA public TO <myosuser>;"
psql -U postgres -d fusionpipe_dev -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO <myosuser>;"
psql -U postgres -d fusionpipe_dev -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO <myosuser>;"
psql -U postgres -d fusionpipe_dev -c "GRANT CREATE ON DATABASE fusionpipe_dev TO <myosuser>;"

psql -U postgres -c "ALTER ROLE <myosuser> CREATEDB;"
```

- Create a database for tests

```bash
createdb fusionpipe_test
```

- Gran permission to your user to write on the test database
```bash
psql -U postgres -d fusionpipe_test -c "GRANT ALL PRIVILEGES ON DATABASE fusionpipe_test TO <carpanes>;"
```

!!! warning "Access control postgresSQL setup"
    By default `postgresSQL` check the permission of for databse access based on your OS username. This is called `peer` modality in postgresSQL gergon.  This behaviour is the dafault one assumed in `fusionpipe`. If you want to change this beheviour you need to change the setting in the file `pg_hba.conf` usually found in the location `/var/lib/pgsql/data/pg_hba.conf`.

### uv python package manager
fusionpipe uses [uv](https://docs.astral.sh/uv/guides/projects/) as a python package manager. 

Install it with 
```bash
pip install uv
```

or following the guideline in ... 


### Install npm
The frontend is develop in Svelte. You need to install `node.js` in order to run it


## Install fusionpipe

- Clone `fusionpipe` repo
```bash
git clone <fusionpipe_url>
```

- Navigate inside the folder
```bash
cd fusionpipe
```

- Install the package dependencies
```bash
uv sync
```

## Set enviroment variables
We recommend to create a `.env` file to set all the environment file necessary to install fusionpipe on your machine.
The file needs to contain the following environment variables.

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

!!! Warning
    `DATABASE_URL_TEST="dbname=<database_test_name> port=<postgres_port>"` in this format assumes that postgresSQL is set-up with `peer` authentication. In case it is set up with username and password the following env variable needs to set-up with `DATABASE_URL_TEST="dbname=<database_test_name> username=<my_postgres_username> password=<my_postgres_password> port=<postgres_port>`

## Start  backend

- Open a new terminal

- Navigate the `fusionpipe` folder

- Set your environment variables from your file 
```bash
source -a
source .env
source +a
```

- Navigate the backend folder 
```bash
cd src/fusionpipe/api
```

- Start the web-service
```bash
uv run python main.py
```

The service can be reached at the `localhost:<backend_port>` that you set in the enviroment variables.

If everythin was installed correctly you should be already capable of running the tests.

## Start the frontend service

- Open another new terminal 

- Navigate the `fusionpipe` folder

- Set your environment variables from your file 
```bash
source -a
source .env
source +a
```

- Navigate the frontend folder
```bash
cd src/fusionpipe/frontend
```

- Start the frontend service in developer mode.
```bash
VITE_BACKEND_HOST=$VITE_BACKEND_HOST VITE_BACKEND_PORT=$VITE_BACKEND_PORT  npm run dev -- --port $VITE_FRONTEND_PORT
```

You should now be able to reach the frontend with your browser at the address `localhost:<frontend_port>` where frontend port is the one specified in your `.env` file.


