# Single-User Installation Guide

This guide provides instructions for installing `fusionpipe` for a local, single-user setup, ideal for individual users and developers.

## Introduction

`fusionpipe` is a lightweight pipeline orchestrator designed to help organize data analysis, simulation, and machine learning pipelines. It consists of the following components:

-   A **frontend** developed in [Svelte](https://svelte.dev/).
-   A **backend** web app using [FastAPI](https://fastapi.tiangolo.com/).
-   A **PostgreSQL** database to track relationships between nodes and pipelines.

!!! warning
    This guide assumes you have `sudo` access on your machine. If not, you may need to contact your IT administrator for assistance with PostgreSQL setup.

This guide is for a bare-metal Unix-like system. A Docker-based installation is currently under development.

## Prerequisites

Before installing `fusionpipe`, ensure you have the following prerequisites installed on your system.

### PostgreSQL

`fusionpipe` uses PostgreSQL to store metadata.

1.  **Install PostgreSQL**
    Follow the official guidelines for your operating system from the [PostgreSQL website](https://www.postgresql.org/download/).

2.  **Configure the Database**
    The following commands will create the necessary databases and grant permissions to your user.

    -   Log in as the `postgres` user:
        ```bash
        sudo -u postgres -i
        ```

    -   Create the main database:
        ```bash
        createdb fusionpipe_dev
        ```

    -   Grant permissions to your OS user (replace `<myuser>` with your actual username):
        ```bash
        psql -U postgres -c "CREATE ROLE <myuser>;"
        psql -U postgres -d fusionpipe_dev -c "GRANT CONNECT ON DATABASE fusionpipe_dev TO <myuser>;"
        psql -U postgres -d fusionpipe_dev -c "GRANT ALL ON SCHEMA public TO <myuser>;"
        psql -U postgres -d fusionpipe_dev -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO <myuser>;"
        psql -U postgres -d fusionpipe_dev -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO <myuser>;"
        psql -U postgres -d fusionpipe_dev -c "GRANT CREATE ON DATABASE fusionpipe_dev TO <myuser>;"
        psql -U postgres -c "ALTER ROLE <myuser> CREATEDB;"
        ```

    -   Create a database for tests:
        ```bash
        createdb fusionpipe_test
        ```

    -   Grant permissions for the test database:
        ```bash
        psql -U postgres -d fusionpipe_test -c "GRANT ALL PRIVILEGES ON DATABASE fusionpipe_test TO <myuser>;"
        ```

!!! warning "PostgreSQL Access Control"
    By default, PostgreSQL uses `peer` authentication, which authenticates users based on their OS username. `fusionpipe` assumes this default behavior. If you need to use a different authentication method (e.g., password-based), you will need to modify the `pg_hba.conf` file, typically located at `/var/lib/pgsql/data/pg_hba.conf`.

### uv (Python Package Manager)

`fusionpipe` uses [uv](https://docs.astral.sh/uv/) for Python package management. Install it using `pip`:
```bash
pip install uv
```

### Node.js and npm

The frontend is a Svelte application, which requires Node.js and npm. Install them by following the official instructions at [nodejs.org](https://nodejs.org/).

## Installation

1.  **Clone the Repository**
    ```bash
    git clone <fusionpipe_url>
    ```

2.  **Navigate to the Project Directory**
    ```bash
    cd fusionpipe
    ```

3.  **Install Dependencies**

    Use `uv` to install the required Python packages:
    ```bash
    uv sync
    ```

## Configuration

### Environment Variables

Create a `.env` file in the root of the project to store the necessary environment variables.

```bash
# .env
FUSIONPIPE_DATA_PATH="<absolute/path/to/fusionpipe/data/folder>"
UV_CACHE_DIR="<absolute/path/to/uv/cache/dir>"
DATABASE_URL="dbname=fusionpipe_dev port=5432"
DATABASE_URL_TEST="dbname=fusionpipe_test port=5432"

BACKEND_HOST="localhost"
BACKEND_PORT=8000

VITE_BACKEND_HOST=${BACKEND_HOST}
VITE_BACKEND_PORT=${BACKEND_PORT}
VITE_FRONTEND_HOST="localhost"
VITE_FRONTEND_PORT=5173

USER_UTILS_FOLDER_PATH="<absolute/path/to/user/utils>"
FP_MATLAB_RUNNER_PATH="<absolute/path/to/matlab/executable>"
VIRTUAL_ENV="<(optional) default virtual env to activate when running the backend>"
```

Here’s an explanation of the variables:

-   `FUSIONPIPE_DATA_PATH`: Absolute path to the folder where `fusionpipe` will store its data.
-   `UV_CACHE_DIR`: Path to the cache directory for `uv`.
-   `DATABASE_URL`: Connection string for the main PostgreSQL database. The example assumes `peer` authentication.
-   `DATABASE_URL_TEST`: Connection string for the test database.
-   `BACKEND_PORT`: Port for the FastAPI backend (e.g., 8000).
-   `VITE_FRONTEND_PORT`: Port for the Svelte frontend (e.g., 5173).
-   `USER_UTILS_FOLDER_PATH`: Absolute path to a folder for shared user utilities.
-   `FP_MATLAB_RUNNER_PATH`: (Optional) Absolute path to your MATLAB executable if you plan to use it.

!!! warning
    If your PostgreSQL setup uses password authentication, you must update the `DATABASE_URL` and `DATABASE_URL_TEST` variables accordingly:
    `DATABASE_URL="dbname=<db_name> user=<user> password=<pass> host=<host> port=<port>"`

## Getting Started

To run `fusionpipe`, you need to start both the backend and frontend services in separate terminals.

### Start the Backend

1.  **Open a new terminal.**
2.  **Navigate to the `fusionpipe` directory.**
3.  **Load the environment variables:**
    ```bash
    source .env
    ```
4.  **Navigate to the backend directory:**
    ```bash
    cd src/fusionpipe/api
    ```
5.  **Start the backend service:**
    ```bash
    uv run python main.py
    ```
The backend will be available at `http://localhost:8000` (or the port you specified).

### Start the Frontend

1.  **Open another new terminal.**
2.  **Navigate to the `fusionpipe` directory.**
3.  **Load the environment variables:**
    ```bash
    source .env
    ```
4.  **Navigate to the frontend directory:**
    ```bash
    cd src/fusionpipe/frontend
    ```
5.  **Install npm dependencies:**
    ```bash
    npm install
    ```
6.  **Start the frontend service in development mode:**
    ```bash
    npm run dev
    ```

You can now access the `fusionpipe` frontend in your browser at `http://localhost:5173` (or the port you specified).