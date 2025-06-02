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
    
    
