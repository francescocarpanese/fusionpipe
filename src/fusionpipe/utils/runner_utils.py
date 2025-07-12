from fusionpipe.utils import db_utils, pip_utils
import os
import subprocess
import time
import ray
from ray.job_submission import JobSubmissionClient
import yaml

def run_node(conn, node_id):
    """
    Run a node based on its run_mode. This wait till the process completes.
    
    Args:
        conn: Database connection
        node_id: ID of the node to run
    """
    run_mode = get_node_run_mode(conn, node_id)
    proc = submit_run_node_with_modality(conn, node_id, run_mode=run_mode)
    if run_mode == "ray":
        wait_ray_job_completion(conn, node_id, proc)
    else:
        wait_subprocess_completion(conn, node_id, proc)

def get_node_run_mode(conn, node_id):
    """
    Get the run_mode parameter from node_parameters.yaml for a given node.
    Defaults to "local" if missing or invalid.
    """
    cur = conn.cursor()
    node_path = db_utils.get_node_folder_path(cur, node_id)
    param_file = os.path.join(node_path, "code", "node_parameters.yaml")
    if not os.path.isfile(param_file):
        raise FileNotFoundError(f"Parameter file not found for node {node_id}: {param_file}")
    with open(param_file, "r") as f:
        params = yaml.safe_load(f)
    run_mode = params.get("run_mode", "local")
    return run_mode


def submit_run_node(conn, node_id):
    """
    Start running a node based on its run_mode.
    
    Args:
        conn: Database connection
        node_id: ID of the node to run
    
    Returns:
        Process or Ray job submission object
    """
    run_mode = get_node_run_mode(conn, node_id)
    try:
        return submit_run_node_with_modality(conn, node_id, run_mode=run_mode)
    except ValueError as e:
        print(f"Error starting node {node_id}: {e}")
        raise RuntimeError(f"Failed to start node {node_id} due to invalid run_mode: {run_mode}")
    

def submit_run_node_with_modality(conn, node_id, run_mode="local"):
    # Initialize Ray if needed
    if run_mode == "ray":
        if not ray.is_initialized():
            init_ray_cluster()
            print("Ray initialized for pipeline execution")

    # Get the database cursor
    cur = conn.cursor()
    if not pip_utils.can_node_run(cur, node_id):
        raise RuntimeError(f"Node {node_id} cannot be run.")
    
    node_path = db_utils.get_node_folder_path(cur, node_id)
    if db_utils.get_node_status(cur, node_id) == "running":
        raise RuntimeError(f"Node {node_id} is already running.")

    print(f"Running node {node_id} in local mode...")
    db_utils.update_node_status(cur, node_id, "running")
    conn.commit()
    proc = None  # Initialize proc to None for error handling

    try:
        log_file = os.path.join(node_path, f"logs.txt")
        start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # Start the process and redirect stdout/stderr to log file
        with open(log_file, "a") as logf:  # Use "a" to append
            # Write process info before starting
            logf.write(f"\n---\nProcess starting\nTime: {start_time}\n")
            # Temporarily write PID as 'TBD', will update after process starts
            logf.flush()
            if run_mode == "local":
                # Run the node's main script using uv
                proc = subprocess.Popen(
                    ["uv", "run", "main.py"],
                    cwd=os.path.join(node_path, "code"),
                    stdout=logf,
                    stderr=subprocess.STDOUT
                )
                # Now write the PID
                logf.write(f"PID: {proc.pid}\n---\n")
                logf.flush()
                # Insert process info into process table
                db_utils.add_process(cur, proc.pid, node_id=node_id, status="running")                
            elif run_mode == "ray":
                # Get the cleint to submit jobs
                client = JobSubmissionClient(os.getenv("RAY_SUBMIT_URL"))

                job_submission = client.submit_job(
                    entrypoint="uv run main.py",
                    runtime_env={
                        "working_dir": os.path.join(node_path, "code"),
                    }
                )
                # Write the Ray job submission info
                logf.write(f"Ray job submitted for node {node_id}, job_id: {job_submission}\n")
                logf.flush()
                db_utils.add_process(cur, job_submission, node_id=node_id, status="running")
                proc = job_submission
        
        conn.commit()
        return proc  # Return the process or Ray job submission object
        
    except Exception as e:
        conn.rollback()
        db_utils.update_node_status(cur, node_id, "failed")
        if proc:
            db_utils.update_process_status(cur, proc.pid, status="failed")
        conn.commit()    
        print(f"Node {node_id} failed to run: {e}")

def check_proc_status(proc):
    """
    Check the status of a process or Ray job.

    Args:
        proc: The subprocess.Popen object or Ray job ID
        run_mode: "local" or "ray"

    Returns:
        str: "running", "completed", or "failed"
    """
    run_mode = "ray" if isinstance(proc, str) and proc.startswith("ray") else "local"
    if run_mode == "local":
        return check_node_status_subprocess(proc)
    elif run_mode == "ray":
        return check_node_status_ray(proc)
    else:
        raise ValueError(f"Unknown run_mode: {run_mode}")


def check_node_status_subprocess(proc):
    """
    Check the status of a subprocess.
    
    Args:
        proc: The subprocess.Popen object
    
    Returns:
        str: "running", "completed", or "failed"
    """
    if proc.poll() is None:
        return "running"  # Process is still running
    elif proc.returncode == 0:
        return "completed"  # Process completed successfully
    else:
        return "failed"  # Process failed with non-zero exit code
    
def check_node_status_ray(job_id):
    """
    Check the status of a Ray job.
    
    Args:
        job_id: The ID of the Ray job
    
    Returns:
        str: "running", "completed", or "failed"
    """
    client = JobSubmissionClient(os.getenv("RAY_SUBMIT_URL"))
    try:
        status = client.get_job_status(job_id)
        if status == "SUCCEEDED":
            return "completed"  # Job completed successfully
        elif status == "RUNNING":
            return "running"  # Job is still running
        elif status in ["FAILED", "CANCELLED", "STOPPED"]:
            return "failed"  # Job failed or was cancelled
    except Exception as e:
        print(f"Error checking Ray job status: {e}")
        return "failed"


def wait_subprocess_completion(conn, node_id, proc):
    proc.wait()
    cur = conn.cursor()
    # Remove process from process table
    db_utils.remove_process(cur, proc.pid)
    if proc.returncode == 0:
        db_utils.update_node_status(cur, node_id, "completed")                
    else:
        db_utils.update_node_status(cur, node_id, "failed")
        db_utils.update_process_status(cur, proc.pid, status="failed")
    conn.commit()

def wait_ray_job_completion(conn, node_id, job_id):
    """
    Wait for a Ray job to complete and update the database status.
    
    Args:
        conn: Database connection
        node_id: ID of the node
        job_id: ID of the Ray job (string)
    """
    cur = conn.cursor()
    try:
        # Get the JobSubmissionClient to check job status
        client = JobSubmissionClient(os.getenv("RAY_SUBMIT_URL"))
        
        # Poll the job status until it's complete
        while True:
            job_status = client.get_job_status(job_id)
            
            if job_status.is_terminal():
                # Job has finished
                if job_status.value == "SUCCEEDED":
                    db_utils.update_node_status(cur, node_id, "completed")
                    db_utils.remove_process(cur, job_id)
                    conn.commit()
                    print(f"Ray job {job_id} for node {node_id} completed successfully")
                    return
                else:
                    # Job failed or was stopped
                    db_utils.update_node_status(cur, node_id, "failed")
                    db_utils.remove_process(cur, job_id)
                    conn.commit()
                    print(f"Ray job {job_id} for node {node_id} failed with status: {job_status.value}")
                    return
            
            # Job is still running, wait a bit before checking again
            time.sleep(1)
    except ray.exceptions.RayJobSubmissionError as e:
        print(f"Ray job submission error for job {job_id}: {e}")
        db_utils.update_node_status(cur, node_id, "failed")
        db_utils.remove_process(cur, job_id)
        conn.commit()

    # After job completion, copy the final Ray job logs to the node's log file
    try:
        job_logs = client.get_job_logs(job_id)
        with open(os.path.join(db_utils.get_node_folder_path(cur, node_id), "logs.txt"), "a") as logf:
          logf.write(f"\n---\nRay job logs for job_id: {job_id}\n{job_logs}\n---\n")
    except Exception as log_err:
        print(f"Could not copy Ray job logs for job {job_id}: {log_err}")

    except Exception as e:
        print(f"Error waiting for Ray job {job_id} completion: {e}")
        db_utils.update_node_status(cur, node_id, "failed")
        db_utils.remove_process(cur, job_id)
        conn.commit()


def run_pipeline(conn, pipeline_id, last_node_id=None, poll_interval=1.0, debug=False):
    """
    Orchestrate the execution of a pipeline.
    - conn: sqlite3.Connection
    - pipeline_id: str
    - poll_interval: float, seconds between polling for new runnable nodes
    """
    from fusionpipe.utils import db_utils, pip_utils

    # Get the database cursor
    cur = conn.cursor()
    all_nodes = set(db_utils.get_all_nodes_from_pip_id(cur, pipeline_id))

    if last_node_id is not None:
        children_nodes = set(pip_utils.get_all_children_nodes(cur, pipeline_id=pipeline_id, node_id=last_node_id))
        all_nodes = all_nodes - children_nodes

    running_nodes = set()
    completed_nodes = set()
    failed_nodes = set()
    running_node_procs = {}

    while True:

        # Handle local mode - update status sets by checking database
        for node_id in all_nodes:
            status = db_utils.get_node_status(cur, node_id)
            if status == "completed":
                completed_nodes.add(node_id)
                running_nodes.discard(node_id)
                running_node_procs.pop(node_id, None)
            elif status == "failed":
                failed_nodes.add(node_id)
                running_nodes.discard(node_id)
                running_node_procs.pop(node_id, None)
            elif status == "running":
                running_nodes.add(node_id)


        if debug:
            print(f"Running nodes: {running_nodes}")
            print(f"Completed nodes: {completed_nodes}")
            print(f"Failed nodes: {failed_nodes}")

        # Find nodes that can run and are not already running/completed/failed
        runnable_nodes = [
            node_id for node_id in all_nodes
            if pip_utils.can_node_run(cur, node_id) and node_id not in (running_nodes | completed_nodes | failed_nodes)
        ]

        # Stop if all nodes are completed or failed, or no more can be scheduled
        if len(completed_nodes | failed_nodes) == len(all_nodes):
            if debug:
                print("Pipeline execution completed - all nodes processed")
            break
        if not runnable_nodes and not running_nodes:
            if debug:
                print("Pipeline execution stopped - no runnable nodes and none running")
            break


        if debug:
            print(f"Runnable nodes: {runnable_nodes}")

        # Start processes for runnable nodes
        for node_id in runnable_nodes:
            try:
                # Run locally (blocking at each node)
                running_node_procs[node_id] = submit_run_node(conn, node_id)
                running_nodes.add(node_id)
                if debug:
                    print(f"Started running node {node_id}")

            except RuntimeError as e:
                print(f"Error running node {node_id}: {e}")
                failed_nodes.add(node_id)
                conn.rollback()

        # Update the status of the nodes depending on the process
        for node_id in running_nodes:
            status = check_proc_status(running_node_procs[node_id])
            db_utils.update_node_status(cur, node_id, status)
            conn.commit()

        time.sleep(poll_interval)


def kill_running_process(conn, node_id, ray_futures=None):
    """
    Kill the running process associated with a node, if any.
    Supports both local processes and Ray tasks.
    
    Args:
        conn: Database connection
        node_id: ID of the node to kill
        ray_futures: Dictionary of Ray futures {node_id: future} (optional)
    """
    cur = conn.cursor()
    node_path = db_utils.get_node_folder_path(cur, node_id)
    log_file = os.path.join(node_path, "logs.txt")
    killed = False
    
    # First, try to kill Ray task if it exists
    if ray_futures and node_id in ray_futures:
        try:
            future = ray_futures[node_id]
            # Cancel the Ray task
            ray.cancel(future)
            
            # Write to log file that the Ray task was cancelled
            with open(log_file, "a") as logf:
                kill_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                logf.write(f"\n---\nRay task cancelled\nTime: {kill_time}\nNode ID: {node_id}\n---\n")
            
            # Update database status
            db_utils.update_node_status(cur, node_id, "failed")
            conn.commit()
            
            # Remove from ray_futures tracking
            ray_futures.pop(node_id)
            print(f"Ray task for node {node_id} cancelled.")
            killed = True
            
        except Exception as e:
            conn.rollback()
            print(f"Failed to cancel Ray task for node {node_id}: {e}")
    
    # Then, try to kill local processes
    proc_ids = db_utils.get_process_ids_by_node(cur, node_id)
    if proc_ids:
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
                killed = True
            except Exception as e:
                conn.rollback()
                print(f"Failed to kill process {pid} for node {node_id}: {e}")
    
    if not killed:
        print(f"No running process or Ray task found for node {node_id}.")


def init_ray_cluster(
        address=None,
        object_store_memory=None,
        temp_dir=None,
        num_cpus=None
    ):
    """
    Initialize Ray cluster with custom configuration
    
    Args:
        address: Ray cluster address
        object_store_memory: Memory size in bytes for object store
        temp_dir: Custom directory for Ray temporary files and object store
        num_cpus: Number of CPUs to use
    """
    if ray.is_initialized():
        print("Ray is already initialized")
        return
    
    init_kwargs = {}
    
    if address:
        init_kwargs['address'] = address
        print(f"Connecting to existing Ray cluster at {address}")
    else:
        print("Starting new Ray cluster")
        
    if object_store_memory:
        init_kwargs['object_store_memory'] = object_store_memory
        print(f"Setting object store memory to {object_store_memory / (1024**3):.2f} GB")
        
    if temp_dir:
        init_kwargs['temp_dir'] = temp_dir
        # Create directory if it doesn't exist
        os.makedirs(temp_dir, exist_ok=True)
        print(f"Using temp directory: {temp_dir}")
        
    if num_cpus:
        init_kwargs['num_cpus'] = num_cpus
        print(f"Using {num_cpus} CPUs")
    
    try:
        ray.init(ignore_reinit_error=True, **init_kwargs)
        print(f"Ray initialized successfully")
    except Exception as e:
        print(f"Failed to initialize Ray: {e}")
        raise


def shutdown_ray_cluster():
    """Shutdown Ray cluster cleanly"""
    if ray.is_initialized():
        ray.shutdown()
        print("Ray cluster shutdown")
    else:
        print("Ray is not initialized")


def get_ray_cluster_info():
    """Get information about the current Ray cluster"""
    if not ray.is_initialized():
        return "Ray is not initialized"
    
    cluster_resources = ray.cluster_resources()
    return {
        "cluster_resources": cluster_resources,
        "dashboard_url": ray.get_dashboard_url(),
        "is_initialized": ray.is_initialized()
    }


def kill_ray_task(node_id, ray_futures):
    """
    Kill a specific Ray task for a node.
    
    Args:
        node_id: ID of the node whose Ray task to kill
        ray_futures: Dictionary of Ray futures {node_id: future}
    
    Returns:
        bool: True if task was successfully cancelled, False otherwise
    """
    if not ray.is_initialized():
        print("Ray is not initialized")
        return False
        
    if node_id not in ray_futures:
        print(f"No Ray task found for node {node_id}")
        return False
    
    try:
        future = ray_futures[node_id]
        ray.cancel(future)
        ray_futures.pop(node_id)
        print(f"Ray task for node {node_id} cancelled successfully")
        return True
    except Exception as e:
        print(f"Failed to cancel Ray task for node {node_id}: {e}")
        return False


def kill_all_pipeline_tasks(conn, pipeline_id, ray_futures=None):
    """
    Kill all running tasks (local and Ray) for a given pipeline.
    
    Args:
        conn: Database connection
        pipeline_id: ID of the pipeline
        ray_futures: Dictionary of Ray futures {node_id: future} (optional)
    
    Returns:
        dict: Summary of killed tasks
    """
    from fusionpipe.utils import db_utils

    
    cur = conn.cursor()
    all_nodes = set(db_utils.get_all_nodes_from_pip_id(cur, pipeline_id))
    
    killed_summary = {
        "ray_tasks_cancelled": 0,
        "local_processes_killed": 0,
        "failed_to_kill": []
    }
    
    for node_id in all_nodes:
        node_status = db_utils.get_node_status(cur, node_id)
        if node_status == "running":
            try:
                # Try to kill Ray task first
                ray_killed = False
                if ray_futures and node_id in ray_futures:
                    if kill_ray_task(node_id, ray_futures):
                        killed_summary["ray_tasks_cancelled"] += 1
                        ray_killed = True
                
                # Try to kill local processes
                proc_ids = db_utils.get_process_ids_by_node(cur, node_id)
                for pid in proc_ids:
                    try:
                        os.kill(int(pid), 9)
                        db_utils.remove_process(cur, pid)
                        killed_summary["local_processes_killed"] += 1
                        print(f"Local process {pid} for node {node_id} killed")
                    except Exception as e:
                        print(f"Failed to kill local process {pid}: {e}")
                        killed_summary["failed_to_kill"].append(f"process_{pid}")
                
                # Update node status
                if ray_killed or proc_ids:
                    db_utils.update_node_status(cur, node_id, "failed")
                    conn.commit()
                    
            except Exception as e:
                print(f"Error killing tasks for node {node_id}: {e}")
                killed_summary["failed_to_kill"].append(node_id)
    
    print(f"Pipeline {pipeline_id} tasks killed:")
    print(f"  Ray tasks cancelled: {killed_summary['ray_tasks_cancelled']}")
    print(f"  Local processes killed: {killed_summary['local_processes_killed']}")
    if killed_summary["failed_to_kill"]:
        print(f"  Failed to kill: {killed_summary['failed_to_kill']}")
    
    return killed_summary


def get_running_ray_tasks(ray_futures):
    """
    Get information about currently running Ray tasks.
    
    Args:
        ray_futures: Dictionary of Ray futures {node_id: future}
    
    Returns:
        dict: Information about running tasks
    """
    if not ray.is_initialized():
        return {"error": "Ray is not initialized"}
    
    running_tasks = {}
    for node_id, future in ray_futures.items():
        try:
            # Check if task is still running (non-blocking)
            ready, not_ready = ray.wait([future], timeout=0)
            if not_ready:  # Task is still running
                running_tasks[node_id] = {
                    "status": "running",
                    "task_id": future.task_id().hex() if hasattr(future, 'task_id') else "unknown"
                }
            else:  # Task is completed
                try:
                    result = ray.get(ready[0])
                    running_tasks[node_id] = {
                        "status": "completed",
                        "result": result
                    }
                except Exception as e:
                    running_tasks[node_id] = {
                        "status": "failed",
                        "error": str(e)
                    }
        except Exception as e:
            running_tasks[node_id] = {
                "status": "error",
                "error": str(e)
            }
    
    return running_tasks