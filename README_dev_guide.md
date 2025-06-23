
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

- Create the table 

```bash
psql -U postgres -d fusionpipe_prod1 -c "CREATE ROLE writers;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT CONNECT, TEMPORARY ON DATABASE fusionpipe_prod1 TO writers;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO writers;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT CREATE ON SCHEMA public TO writers;"
psql -U postgres -d fusionpipe_prod1 -c "GRANT writers TO carpanes;"
```

- Grant access to the user.

- Export the env variable
`export $(grep -v '^#' something.env | xargs)`


- Start the backend
Making sure you are in a terminal where you have the right enviroment path.

- Start the frontend

Probably some of them needs to be read from env variables.
`VITE_BACKEND_HOST=localhost VITE_BACKEND_PORT=8100 npm run dev -- --port 5174`


# Create a new user
