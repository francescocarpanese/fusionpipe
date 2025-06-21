from fusionpipe.utils import db_utils, pip_utils
import os
import subprocess
import time

def run_node(conn, node_id, run_mode="local"):
    cur = conn.cursor()
    if not pip_utils.can_node_run(cur, node_id):
        raise RuntimeError(f"Node {node_id} cannot be run.")
    
    node_path = db_utils.get_node_folder_path(cur, node_id)
    if db_utils.get_node_status(cur, node_id) == "running":
        raise RuntimeError(f"Node {node_id} is already running.")
    if run_mode == "local":
        print(f"Running node {node_id} in local mode...")
        db_utils.update_node_status(cur, node_id, "running")
        conn.commit()
        try:
            log_file = os.path.join(node_path, f"logs.txt")
            start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # Start the process and redirect stdout/stderr to log file
            with open(log_file, "a") as logf:  # Use "a" to append
                # Write process info before starting
                logf.write(f"\n---\nProcess starting\nTime: {start_time}\n")
                # Temporarily write PID as 'TBD', will update after process starts
                logf.flush()
                proc = subprocess.Popen(
                    ["uv", "run", "python", "main.py"],
                    cwd=os.path.join(node_path, "code"),
                    stdout=logf,
                    stderr=subprocess.STDOUT
                )
                # Now write the PID
                logf.write(f"PID: {proc.pid}\n---\n")
                logf.flush()
            # Insert process info into process table
            db_utils.add_process(cur, proc.pid, node_id=node_id, status="running")
            conn.commit()
            proc.wait()
            # Remove process from process table
            db_utils.remove_process(cur, proc.pid)
            if proc.returncode == 0:
                db_utils.update_node_status(cur, node_id, "completed")                
            else:
                db_utils.update_node_status(cur, node_id, "failed")
                db_utils.update_process_status(cur, proc.pid, status="failed")
            conn.commit()
        except Exception as e:
            db_utils.update_node_status(cur, node_id, "failed")
            db_utils.update_process_status(cur, proc.pid, status="failed")
            conn.rollback()
            print(f"Node {node_id} failed to run: {e}")
    elif run_mode == "ray":
        # To be implemented
        return


def run_pipeline(conn, pipeline_id, last_node_id=None, run_mode="local", poll_interval=1.0, debug=False):
    """
    Orchestrate the execution of a pipeline.
    - conn: sqlite3.Connection
    - pipeline_id: str
    - poll_interval: float, seconds between polling for new runnable nodes
    """
    from fusionpipe.utils import db_utils, pip_utils

    cur = conn.cursor()
    all_nodes = set(db_utils.get_all_nodes_from_pip_id(cur, pipeline_id))

    if last_node_id is not None:
        children_nodes = set(pip_utils.get_all_children_nodes(cur, pipeline_id=pipeline_id, node_id=last_node_id))
        all_nodes = all_nodes - children_nodes

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

        # Start processes for runnable nodes.
        # For the moment this is run sequentially
        for node_id in runnable_nodes:
            try:
                run_node(conn, node_id, run_mode=run_mode)
                running_nodes.add(node_id)
                if debug:
                    print(f"Started running node {node_id}")
            except RuntimeError as e:
                print(f"Error running node {node_id}: {e}")
                failed_nodes.add(node_id)
                conn.rollback()
        
        # Stop if all nodes are completed or failed, or no more can be scheduled
        if len(completed_nodes | failed_nodes) == len(all_nodes):
            break
        if not runnable_nodes and not running_nodes:
            break

        time.sleep(poll_interval)


def kill_running_process(conn, node_id):
    """
    Kill the running process associated with a node, if any.
    """
    cur = conn.cursor()
    proc_ids = db_utils.get_process_ids_by_node(cur, node_id)
    node_path = db_utils.get_node_folder_path(cur, node_id)
    log_file = os.path.join(node_path, "logs.txt")
    if not proc_ids:
        print(f"No running process found for node {node_id}.")
        return
    for pid in proc_ids:
        try:
            os.kill(int(pid), 9)
            # Write to log file that the process was killed
            with open(log_file, "a") as logf:
                kill_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                logf.write(f"\n---\nProcess killed\nTime: {kill_time}\nPID: {pid}\n---\n")
            db_utils.update_node_status(cur, node_id, "failed")
            db_utils.remove_process(cur, pid)
            conn.commit()
            print(f"Process {pid} for node {node_id} killed.")
        except Exception as e:
            conn.rollback()
            print(f"Failed to kill process {pid} for node {node_id}: {e}")