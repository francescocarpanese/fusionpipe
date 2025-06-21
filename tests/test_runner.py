import pytest
import tempfile
import os
import sqlite3
import time
from fusionpipe.utils import db_utils, pip_utils, runner_utils


@pytest.mark.parametrize("node_init_status,expected_status", [
    ("ready", "completed"),
    ("running", "running"),
    ("failed", "failed"),
    ("completed", "completed"),
])
def test_run_node_creates_and_runs_node(pg_test_db, tmp_base_dir, node_init_status, expected_status):

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status=node_init_status)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()


    # Check that no process exists for this node before running
    processes_before = db_utils.get_processes_by_node(cur, node_id)
    assert not processes_before, f"Expected no process for node {node_id} before running, found: {processes_before}"

    error_message = None
    try:
        runner_utils.run_node(conn, node_id, run_mode="local")
    except Exception as e:
        conn.rollback()
        error_message = str(e)

    status = db_utils.get_node_status(cur, node_id)
    assert status == expected_status, f"Expected status '{expected_status}', got '{status}'"

    # Check process table after running
    processes_after = db_utils.get_processes_by_node(cur, node_id)
    if expected_status == "completed":
        # Process should be removed after successful completion
        assert not processes_after, f"Expected no process for node {node_id} after completion, found: {processes_after}"

    if error_message:
        print(f"Caught error: {error_message}")

    conn.close()



@pytest.mark.parametrize("last_node,expected_status_a,expected_status_b,expected_status_c", [
    (None, "completed", "completed", "completed"),
    (0, "completed", "ready", "ready"),
    (1, "completed", "completed", "ready"),
    (2, "completed", "completed", "completed"),
])
def test_run_pipeline(pg_test_db, tmp_path, last_node, expected_status_a, expected_status_b, expected_status_c):
    from fusionpipe.utils import db_utils, pip_utils, runner_utils

    # Setup DB
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create pipeline and two nodes (A -> B)
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    node_a = pip_utils.generate_node_id()
    node_b = pip_utils.generate_node_id()
    node_c = pip_utils.generate_node_id()
    node_ids = [node_a, node_b, node_c] 
    folder_a = os.path.join(tmp_path, node_a)
    folder_b = os.path.join(tmp_path, node_b)
    folder_c = os.path.join(tmp_path, node_c)    

    db_utils.add_node_to_nodes(cur, node_id=node_a, status="ready", editable=True, folder_path=folder_a)
    db_utils.add_node_to_nodes(cur, node_id=node_b, status="ready", editable=True, folder_path=folder_b)
    db_utils.add_node_to_nodes(cur, node_id=node_c, status="ready", editable=True, folder_path=folder_c)
    db_utils.add_node_to_pipeline(cur, node_id=node_a, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_b, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_c, pipeline_id=pipeline_id)    
    db_utils.add_node_relation(cur, child_id=node_b, parent_id=node_a)
    db_utils.add_node_relation(cur, child_id=node_c, parent_id=node_b)
    pip_utils.init_node_folder(folder_path_nodes=folder_a)
    pip_utils.init_node_folder(folder_path_nodes=folder_b)
    pip_utils.init_node_folder(folder_path_nodes=folder_c)    
    conn.commit()

    if last_node is not None:
        # Run pipeline from last node
        runner_utils.run_pipeline(conn, pipeline_id, last_node_id=node_ids[last_node], run_mode="local", poll_interval=0.2, debug=True)
    else:
        runner_utils.run_pipeline(conn, pipeline_id, run_mode="local", poll_interval=0.2, debug=True)

    # Assert both nodes are completed
    status_a = db_utils.get_node_status(cur, node_a)
    status_b = db_utils.get_node_status(cur, node_b)
    status_c = db_utils.get_node_status(cur, node_c)    
    assert status_a == expected_status_a
    assert status_b == expected_status_b
    assert status_c == expected_status_c

    conn.close()


def test_kill_running_process(pg_test_db, tmp_base_dir):
    # This test is failing because of concurrency. It will be fixed when migrating the database
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    code_dir = os.path.join(folder_path_nodes, "code")
    os.makedirs(code_dir, exist_ok=True)
    # Write a long-running dummy script
    main_py = os.path.join(code_dir, "main.py")
    with open(main_py, "w") as f:
        f.write("import time\ntime.sleep(10)\n")
    conn.commit()

    # Start the node in a separate process/thread
    import threading
    def run_node():
        try:
            # Re-initialize schema and data if needed, or use the same DB file if not in-memory
            runner_utils.run_node(conn, node_id, run_mode="local")
        except Exception:
            pass
    t = threading.Thread(target=run_node)
    t.start()
    time.sleep(1)  # Give it time to start and insert process

    # Check process is running
    processes = db_utils.get_processes_by_node(cur, node_id)
    assert processes, f"Expected process for node {node_id} to be running, found none"
    # Kill the running process
    runner_utils.kill_running_process(conn, node_id)
    # Rollback to recover from any aborted transaction state
    conn.rollback()
    # Check process is removed and node is failed
    processes_after = db_utils.get_processes_by_node(cur, node_id)
    assert not processes_after, f"Expected no process for node {node_id} after kill, found: {processes_after}"
    status = db_utils.get_node_status(cur, node_id)
    assert status == "failed", f"Expected node status 'failed' after kill, got '{status}'"
    t.join(timeout=2)
    conn.close()