from fusionpipe.utils import db_utils, pip_utils
import os
import subprocess
import time

def run_node(conn, node_id, run_mode="local"):
    cur = conn.cursor()
    if not pip_utils.can_node_run(cur, node_id):
        raise RuntimeError(f"Node {node_id} cannot be run.")
    
    node_path = db_utils.get_node_folder_path(cur, node_id)
    if run_mode == "local":
        # Simulate running the node locally
        print(f"Running node {node_id} in local mode...")
        db_utils.update_node_status(cur, node_id, "running")
        conn.commit()
        try:
            subprocess.run(["python", os.path.join(node_path, "code", "run.py")], check=True)
            db_utils.update_node_status(cur, node_id, "completed")
            conn.commit()
        except subprocess.CalledProcessError as e:
            db_utils.update_node_status(cur, node_id, "failed")
            conn.commit()
            print(f"Node {node_id} failed to run: {e}")
    elif run_mode == "ray":
        # To be implemented
        return


def run_pipeline(conn, pipeline_id, run_mode="local", poll_interval=1.0, debug=False):
    """
    Orchestrate the execution of a pipeline.
    - conn: sqlite3.Connection
    - pipeline_id: str
    - poll_interval: float, seconds between polling for new runnable nodes
    """
    from fusionpipe.utils import db_utils, pip_utils

    cur = conn.cursor()
    all_nodes = set(db_utils.get_all_nodes_from_pip_id(cur, pipeline_id))
    running_nodes = set()
    completed_nodes = set()
    failed_nodes = set()

    while True:
        cur = conn.cursor()
        # Update status sets
        for node_id in all_nodes:
            status = db_utils.get_node_status(cur, node_id)
            if status == "completed":
                completed_nodes.add(node_id)
            elif status == "failed":
                failed_nodes.add(node_id)
            elif status == "running":
                running_nodes.add(node_id)

        if debug:
            print(f"Running nodes: {running_nodes}")
            print(f"Completed nodes: {completed_nodes}")
            print(f"Failed nodes: {failed_nodes}")

        # Find nodes that can run and are not already running/completed/failed
        runnable_nodes = [
            node_id for node_id in all_nodes
            if pip_utils.can_node_run(cur, node_id)
        ]

        if debug:
            print(f"Runnable nodes: {runnable_nodes}")

        # Start processes for runnable nodes
        for node_id in runnable_nodes:
            run_node(conn, node_id, run_mode=run_mode)
            running_nodes.add(node_id)
            if debug:
                print(f"Started running node {node_id}")
        
        # Stop if all nodes are completed or failed, or no more can be scheduled
        if len(completed_nodes | failed_nodes) == len(all_nodes):
            break
        if not runnable_nodes and not running_nodes:
            break

        time.sleep(poll_interval)