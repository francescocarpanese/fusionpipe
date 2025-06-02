import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from fusionpipe.api.routes import pipeline_handle
from fastapi import FastAPI
import os
import os


@pytest.fixture
def api_fixture(tmp_database_path):
    # Get a temporary database path, create the database and initialise
    from fusionpipe.utils.db_utils import create_db
    os.environ["FUSIONPIPE_DB_PATH"] = tmp_database_path
    conn = create_db(tmp_database_path)
    app = FastAPI()
    app.include_router(pipeline_handle.router)
    yield TestClient(app), conn


def test_create_pipeline_success(api_fixture):
    client, conn = api_fixture
    from fusionpipe.utils.db_utils import check_pipeline_exists
    response = client.post("/create_pipeline")
    assert response.status_code == 200
    data = response.json()
    assert "pipeline_id" in data
    cur = conn.cursor()
    assert check_pipeline_exists(cur, data["pipeline_id"])
    


def test_get_all_pipeline_ids_success(api_fixture):
    from fusionpipe.utils.db_utils import add_pipeline
    client, conn = api_fixture

    # Insert test pipelines into the database
    cur = conn.cursor()
    pipeline_ids = ["pipeline_1", "pipeline_2", "pipeline_3"]
    for pipeline_id in pipeline_ids:
        add_pipeline(cur, pipeline_id)
    conn.commit()

    # Test the endpoint
    response = client.get("/get_all_pipeline_ids")
    assert response.status_code == 200
    data = response.json()
    assert "pip_ids" in data
    assert set(data["pip_ids"]) == set(pipeline_ids)


def test_get_all_pipeline_ids_no_pipelines(api_fixture):
    client, conn = api_fixture

    # Ensure the database is empty
    cur = conn.cursor()
    cur.execute("DELETE FROM pipelines")  # Assuming a table named 'pipelines'
    conn.commit()

    # Test the endpoint
    response = client.get("/get_all_pipeline_ids")
    data = response.json()
    assert data["pip_ids"] == []


def test_get_all_pipeline_ids_server_error(api_fixture):
    client, conn = api_fixture

    # Simulate a server error by mocking the database utility function
    with patch("fusionpipe.utils.db_utils.get_all_pipeline_ids", side_effect=Exception("Database error")):
        response = client.get("/get_all_pipeline_ids")
        assert response.status_code == 500
        data = response.json()
        assert data["detail"] == "Database error"
