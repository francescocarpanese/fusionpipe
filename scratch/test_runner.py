from fusionpipe.utils import db_utils, pip_utils
import os
import subprocess


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
        except subprocess.CalledProcessError as e:
            db_utils.update_node_status(cur, node_id, "failed")
            print(f"Node {node_id} failed to run: {e}")
        conn.commit()
    elif run_mode == "remote":
        # To be implemented
        return


