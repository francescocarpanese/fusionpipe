import pytest
import tempfile
import os
import sqlite3
from fusionpipe.utils import db_utils, pip_utils, runner_utils


@pytest.mark.parametrize("node_init_status,expected_status", [
    ("ready", "completed"),
    ("running", "running"),
    ("failed", "failed"),
    ("completed", "completed"),
])
def test_run_node_creates_and_runs_node(in_memory_db_conn, tmp_base_dir, node_init_status, expected_status):

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    node_folder_path = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=node_folder_path, status=node_init_status)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(node_folder_path=node_folder_path)
    conn.commit()

    error_message = None
    try:
        runner_utils.run_node(conn, node_id, run_mode="local")
    except Exception as e:
        error_message = str(e)

    status = db_utils.get_node_status(cur, node_id)
    assert status == expected_status, f"Expected status '{expected_status}', got '{status}'"
    # Optionally, you can assert on error_message if you want to check for specific errors
    # For now, just print it if exists
    if error_message:
        print(f"Caught error: {error_message}")

    conn.close()




def test_run_pipeline_simple(in_memory_db_conn, tmp_path):
    from fusionpipe.utils import db_utils, pip_utils, runner_utils

    # Setup DB
    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create pipeline and two nodes (A -> B)
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    node_a = pip_utils.generate_node_id()
    node_b = pip_utils.generate_node_id()
    folder_a = os.path.join(tmp_path, node_a)
    folder_b = os.path.join(tmp_path, node_b)

    db_utils.add_node_to_nodes(cur, node_id=node_a, status="ready", editable=True, folder_path=folder_a)
    db_utils.add_node_to_nodes(cur, node_id=node_b, status="ready", editable=True, folder_path=folder_b)
    db_utils.add_node_to_pipeline(cur, node_id=node_a, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_b, pipeline_id=pipeline_id)
    db_utils.add_node_relation(cur, child_id=node_b, parent_id=node_a)
    pip_utils.init_node_folder(node_folder_path=folder_a)
    pip_utils.init_node_folder(node_folder_path=folder_b)
    conn.commit()

    # Run the pipeline
    runner_utils.run_pipeline(conn, pipeline_id, run_mode="local", poll_interval=0.2, debug=True)

    # Assert both nodes are completed
    status_a = db_utils.get_node_status(cur, node_a)
    status_b = db_utils.get_node_status(cur, node_b)
    assert status_a == "completed"
    assert status_b == "completed"

    conn.close()