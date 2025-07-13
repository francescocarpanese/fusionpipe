from fusionpipe.utils import db_utils, pip_utils
import os
import subprocess
import time
import ray
from ray.job_submission import JobSubmissionClient
import yaml
from contextlib import contextmanager

def run_node(conn, node_id):
    """
    Run a node based on its run_mode. This wait till the process completes.
    
    Args:
        conn: Database connection
        node_id: ID of the node to run
    """
    proc = submit_run_node(conn, node_id)
    run_mode = get_run_mode_from_params(conn, node_id)
    if run_mode == "ray":
        wait_ray_job_completion(conn, node_id, proc)
    else:
        wait_subprocess_completion(conn, node_id, proc)

def get_run_mode_from_params(conn, node_id):
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
        
    Raises:
        RuntimeError: If submission fails for any reason
        FileNotFoundError: If parameter file is not found
        ValueError: If run_mode is invalid
    """
    try:
        run_mode = get_run_mode_from_params(conn, node_id)
        return submit_node_with_run_mode(conn, node_id, run_mode=run_mode)
    except (ValueError, FileNotFoundError) as e:
        # These are validation errors - don't retry
        print(f"Validation error for node {node_id}: {e}")
        raise
    except Exception as e:
        # These are submission errors
        print(f"Submission failed for node {node_id}: {e}")
        raise RuntimeError(f"Failed to submit node {node_id}: {e}")
    
@contextmanager
def node_execution_context(conn, node_id, run_mode):
    """
    Context manager for safe node execution with automatic cleanup.
    
    Args:
        conn: Database connection
        node_id: ID of the node to execute
        run_mode: Execution mode ("local" or "ray")
    
    Yields:
        tuple: (cursor, node_path, log_file, proc_ref)
    """
    cur = conn.cursor()
    proc_ref = {"proc": None}  # Use dict to allow modification in nested scope
    node_path = db_utils.get_node_folder_path(cur, node_id)
    already_running_or_completed = False

    try:
        # Prepare log file
        log_file = os.path.join(node_path, "logs.txt")

        # If it was already running or completed, skip the submission.
        if db_utils.get_node_status(cur, node_id) == "running" or db_utils.get_node_status(cur, node_id) == "completed":
            already_running_or_completed = True
            raise RuntimeError(f"Node {node_id} is already running or completed.")

        # Pre-execution validation
        if not pip_utils.can_node_run(cur, node_id):
            raise RuntimeError(f"Node {node_id} cannot be run.")
        
        # Initialize Ray if needed
        if run_mode == "ray" and not ray.is_initialized():
            init_ray_cluster()
            print("Ray initialized for pipeline execution")
        
        # Set node to running state
        print(f"Running node {node_id} in {run_mode} mode...")
        db_utils.update_node_status(cur, node_id, "running")
        conn.commit()
    
        yield cur, node_path, log_file, proc_ref
        
    except Exception as e:
        # Cleanup on any error
        conn.rollback()

        if already_running_or_completed:
            print(f"Node {node_id} is already running or completed, skipping cleanup.")
            raise RuntimeError(f"Node {node_id} is already running or completed, cannot submit again.")

        if not already_running_or_completed:
            db_utils.update_node_status(cur, node_id, "failed")
            
            # Handle process cleanup based on what type of process we have
            proc = proc_ref.get("proc")
            if proc:
                try:
                    if isinstance(proc, subprocess.Popen):
                        # Local process cleanup
                        try:
                            proc.terminate()  # Try graceful termination first
                            time.sleep(0.5)
                            if proc.poll() is None:
                                proc.kill()  # Force kill if still running
                            db_utils.update_process_status(cur, proc.pid, status="failed")
                        except Exception as cleanup_error:
                            print(f"Warning: Failed to cleanup local process for node {node_id}: {cleanup_error}")
                    elif isinstance(proc, str):
                        # Ray job cleanup
                        try:
                            client = JobSubmissionClient(os.getenv("RAY_SUBMIT_URL"))
                            client.stop_job(proc)
                            db_utils.update_process_status(cur, proc, status="failed")
                        except Exception as cleanup_error:
                            print(f"Warning: Failed to cleanup Ray job for node {node_id}: {cleanup_error}")
                except Exception as e:
                    print(f"Error during process cleanup for node {node_id}: {e}")
        
            conn.commit()
            print(f"Node {node_id} failed to run: {e}")
            raise


def _write_execution_start_log(log_file, node_id, run_mode):
    """Write execution start information to log file"""
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open(log_file, "a") as logf:
        logf.write(f"\n---\nNode {node_id} execution starting\n")
        logf.write(f"Time: {start_time}\n")
        logf.write(f"Run mode: {run_mode}\n")
        logf.flush()

def _create_local_process(cur, node_id, node_path, log_file):
    """
    Create and start a local subprocess
    
    Raises:
        RuntimeError: If process creation fails
        FileNotFoundError: If working directory doesn't exist
    """
    working_dir = os.path.join(node_path, "code")
    
    # Validate working directory exists
    if not os.path.exists(working_dir):
        raise FileNotFoundError(f"Working directory does not exist: {working_dir}")
    
    # Validate main.py exists
    main_py_path = os.path.join(working_dir, "main.py")
    if not os.path.exists(main_py_path):
        raise FileNotFoundError(f"main.py not found in {working_dir}")
    
    try:
        with open(log_file, "a") as logf:
            proc = subprocess.Popen(
                ["uv", "run", "main.py"],
                cwd=working_dir,
                stdout=logf,
                stderr=subprocess.STDOUT
            )
            
            # Log process information
            logf.write(f"PID: {proc.pid}\n---\n")
            logf.flush()
            
            # Verify process started successfully
            poll_result = proc.poll()
            if poll_result is not None and poll_result != 0:
                raise RuntimeError(f"Process terminated immediately with return code {proc.returncode}")
            
            # Add to database
            db_utils.add_process(cur, proc.pid, node_id=node_id, status="running")
            return proc
            
    except subprocess.SubprocessError as e:
        raise RuntimeError(f"Failed to start subprocess: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error creating local process: {e}")


def _create_ray_job(cur, node_id, node_path, log_file):
    """
    Create and submit a Ray job
    
    Raises:
        RuntimeError: If Ray job submission fails
        FileNotFoundError: If working directory doesn't exist
        ConnectionError: If Ray cluster is not accessible
    """
    working_dir = os.path.join(node_path, "code")
    
    # Validate working directory exists
    if not os.path.exists(working_dir):
        raise FileNotFoundError(f"Working directory does not exist: {working_dir}")
    
    # Validate main.py exists
    main_py_path = os.path.join(working_dir, "main.py")
    if not os.path.exists(main_py_path):
        raise FileNotFoundError(f"main.py not found in {working_dir}")
    
    # Validate Ray submit URL
    ray_submit_url = os.getenv("RAY_SUBMIT_URL")
    if not ray_submit_url:
        raise RuntimeError("RAY_SUBMIT_URL environment variable is not set")
    
    try:
        client = JobSubmissionClient(ray_submit_url)
        
        # Create unique submission ID with timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        submission_id = f"ray_{node_id}_{timestamp}"
        
        job_submission = client.submit_job(
            entrypoint="uv run main.py",
            runtime_env={"working_dir": working_dir},
            submission_id=submission_id,
        )
        
        # Validate job submission response
        if not job_submission or job_submission == "":
            raise RuntimeError("Ray job submission returned empty job ID")
        
        # Log Ray job information
        with open(log_file, "a") as logf:
            logf.write(f"Ray job submitted: {job_submission}\n---\n")
            logf.flush()
        
        # Add to database
        db_utils.add_process(cur, job_submission, node_id=node_id, status="running")
        
        # Verify job was actually submitted by checking its status
        try:
            job_status = client.get_job_status(job_submission)
            if job_status is None:
                raise RuntimeError(f"Job {job_submission} not found after submission")
        except Exception as e:
            raise RuntimeError(f"Failed to verify job submission: {e}")
        
        return job_submission
        
    except ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Ray cluster at {ray_submit_url}: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error creating Ray job: {e}")

def submit_node_with_run_mode(conn, node_id, run_mode="local"):
    """
    Submit a node for execution with the specified run mode.
    
    Args:
        conn: Database connection
        node_id: ID of the node to run
        run_mode: Execution mode ("local" or "ray")
    
    Returns:
        Process object (subprocess.Popen for local, job_id string for ray)
    
    Raises:
        RuntimeError: If node cannot be run, is already running, or submission fails
        ValueError: If run_mode is invalid
    """
    # Validate run mode
    if run_mode not in ["local", "ray"]:
        raise ValueError(f"Invalid run_mode: {run_mode}. Must be 'local' or 'ray'")
    
    with node_execution_context(conn, node_id, run_mode) as (cur, node_path, log_file, proc_ref):
        try:
            # Write execution start log
            _write_execution_start_log(log_file, node_id, run_mode)
            
            # Create and start the process based on run mode
            proc = None
            if run_mode == "local":
                proc = _create_local_process(cur, node_id, node_path, log_file)
                if proc is None:
                    raise RuntimeError(f"Failed to create local process for node {node_id}")
                # Verify process is actually running
                if proc.poll() is not None:
                    raise RuntimeError(f"Local process for node {node_id} terminated immediately with return code {proc.returncode}")
                    
            elif run_mode == "ray":
                proc = _create_ray_job(cur, node_id, node_path, log_file)
                if proc is None or proc == "":
                    raise RuntimeError(f"Failed to create Ray job for node {node_id}")
                # Verify Ray job was submitted successfully
                try:
                    client = JobSubmissionClient(os.getenv("RAY_SUBMIT_URL"))
                    job_status = client.get_job_status(proc)
                    if job_status is None:
                        raise RuntimeError(f"Ray job {proc} for node {node_id} was not found after submission")
                except Exception as e:
                    raise RuntimeError(f"Failed to verify Ray job submission for node {node_id}: {e}")
            
            # Store proc in the reference for potential cleanup
            proc_ref["proc"] = proc
            
            # Final validation before committing
            if proc is None:
                raise RuntimeError(f"Process creation returned None for node {node_id}")
            
            # Commit transaction
            conn.commit()
            print(f"Successfully submitted node {node_id} with {run_mode} mode")
            return proc
            
        except Exception as e:
            # Re-raise with more context
            raise RuntimeError(f"Submission failed for node {node_id} in {run_mode} mode: {e}")








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


def run_pipeline(conn, pipeline_id, last_node_id=None, poll_interval=1.0, debug=False, max_concurrent_nodes=None, timeout=None, on_progress=None):
    """
    Run a pipeline with improved error handling, concurrency control, and progress tracking.
    
    Args:
        conn: Database connection
        pipeline_id: ID of the pipeline to run
        last_node_id: Stop execution after this node (exclude its children)
        poll_interval: Time between status checks (seconds)
        debug: Enable debug logging
        max_concurrent_nodes: Maximum number of nodes to run concurrently (None for unlimited)
        timeout: Maximum total execution time in seconds (None for no timeout)
        on_progress: Callback function called on progress updates: on_progress(completed, failed, running, total)
    
    Returns:
        dict: Execution summary with counts and timing information
    
    Raises:
        TimeoutError: If execution exceeds timeout
        RuntimeError: If pipeline execution fails critically
    """
    from fusionpipe.utils import db_utils, pip_utils
    import time
    
    start_time = time.time()
    
    # Validate inputs
    _validate_pipeline_inputs(max_concurrent_nodes, timeout)

    # Get the database cursor
    cur = conn.cursor()
    
    # Initialize pipeline state
    all_nodes, excluded_nodes = _initialize_pipeline_state(cur, pipeline_id, last_node_id)
    
    # Handle empty pipeline
    if not all_nodes:
        if debug:
            print(f"Pipeline {pipeline_id} has no nodes")
        return _create_execution_summary(set(), set(), excluded_nodes, set(), 0.0)

    running_nodes = set()
    completed_nodes = set()
    failed_nodes = set()
    running_node_procs = {}  # Collector for subprocesses handle and submission_ids ray
    last_progress_report = 0
    consecutive_errors = 0
    max_consecutive_errors = 5

    try:
        while True:
            current_time = time.time()
            
            # Check timeout
            if timeout and (current_time - start_time) > timeout:
                # Kill running processes before timeout
                _handle_timeout_cleanup(conn, running_nodes, failed_nodes, running_node_procs, timeout, debug)

            try:
                # Handle status updates - update status sets by checking database
                _update_node_status_from_database(cur, all_nodes, running_nodes, completed_nodes, failed_nodes)

                # Update running nodes status
                _update_running_nodes_status(conn, cur, running_nodes, completed_nodes, failed_nodes, running_node_procs, debug)

                consecutive_errors = 0  # Reset on successful status check

            except Exception as e:
                consecutive_errors += 1
                if debug:
                    print(f"Error in status checking loop (attempt {consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    raise RuntimeError(f"Too many consecutive errors ({consecutive_errors}) in pipeline execution")
                
                # Continue with degraded functionality
                time.sleep(poll_interval)
                continue

            # Progress reporting
            total_processed = len(completed_nodes | failed_nodes)
            if on_progress and (total_processed != last_progress_report):
                on_progress(len(completed_nodes), len(failed_nodes), len(running_nodes), len(all_nodes))
                last_progress_report = total_processed

            if debug:
                print(f"Running nodes: {running_nodes}")
                print(f"Completed nodes: {completed_nodes}")
                print(f"Failed nodes: {failed_nodes}")

            # Find the new nodes that can run and are not already running/completed/failed
            runnable_nodes = _find_runnable_nodes(cur, all_nodes, running_nodes, completed_nodes, failed_nodes, max_concurrent_nodes, debug)

            # Check if execution is complete
            if _check_execution_complete(all_nodes, completed_nodes, failed_nodes, runnable_nodes, running_nodes, debug):
                break

            if debug and runnable_nodes:
                print(f"Runnable nodes: {runnable_nodes}")

            # Start processes for runnable nodes
            _start_runnable_nodes(conn, cur, runnable_nodes, running_nodes, failed_nodes, running_node_procs, debug)

            time.sleep(poll_interval)

    except (KeyboardInterrupt, TimeoutError):
        # Graceful shutdown - kill all running processes
        _handle_interrupt_cleanup(conn, running_nodes, failed_nodes, running_node_procs, debug)
        raise
    
    finally:
        # Final progress report
        if on_progress:
            on_progress(len(completed_nodes), len(failed_nodes), len(running_nodes), len(all_nodes))

    # Execution summary
    execution_time = time.time() - start_time
    summary = _create_execution_summary(completed_nodes, failed_nodes, excluded_nodes, all_nodes, execution_time)
    
    if debug:
        print(f"Pipeline execution summary: {summary}")
    
    return summary

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
                    except Exception as e:
                        print(f"Failed to kill process {pid} for node {node_id}: {e}")
                
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
                        "result": str(result)
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

def _validate_pipeline_inputs(max_concurrent_nodes, timeout):
    """
    Validate input parameters for pipeline execution.
    
    Args:
        max_concurrent_nodes: Maximum number of nodes to run concurrently
        timeout: Maximum total execution time in seconds
    
    Raises:
        ValueError: If inputs are invalid
    """
    if max_concurrent_nodes is not None and max_concurrent_nodes <= 0:
        raise ValueError("max_concurrent_nodes must be positive")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be positive")


def _initialize_pipeline_state(cur, pipeline_id, last_node_id=None):
    """
    Initialize pipeline state and validate pipeline exists.
    
    Args:
        cur: Database cursor
        pipeline_id: ID of the pipeline to run
        last_node_id: Stop execution after this node (exclude its children)
    
    Returns:
        tuple: (all_nodes, excluded_nodes) as sets
    
    Raises:
        ValueError: If pipeline doesn't exist or last_node_id is invalid
    """
    # Validate pipeline exists
    if not db_utils.check_pipeline_exists(cur, pipeline_id):
        raise ValueError(f"Pipeline {pipeline_id} does not exist")
    
    all_nodes = set(db_utils.get_all_nodes_from_pip_id(cur, pipeline_id))
    
    # If last_node_id is provided, exclude its children from the execution
    excluded_nodes = set()
    if last_node_id is not None:
        if last_node_id not in all_nodes:
            raise ValueError(f"last_node_id {last_node_id} is not in pipeline {pipeline_id}")
        children_nodes = set(pip_utils.get_all_children_nodes(cur, pipeline_id=pipeline_id, node_id=last_node_id))
        excluded_nodes = children_nodes
        all_nodes = all_nodes - excluded_nodes
    
    return all_nodes, excluded_nodes


def _update_node_status_from_database(cur, all_nodes, running_nodes, completed_nodes, failed_nodes):
    """
    Update node status sets by checking database for non-running nodes.
    
    Args:
        cur: Database cursor
        all_nodes: Set of all nodes in pipeline
        running_nodes: Set of currently running nodes
        completed_nodes: Set of completed nodes
        failed_nodes: Set of failed nodes
    """
    for node_id in list(all_nodes):  # Use list() to avoid runtime modification
        if node_id in running_nodes or node_id in completed_nodes or node_id in failed_nodes:
            continue
            
        status = db_utils.get_node_status(cur, node_id)
        if status == "completed":
            completed_nodes.add(node_id)
        elif status == "failed":
            failed_nodes.add(node_id)


def _update_running_nodes_status(conn, cur, running_nodes, completed_nodes, failed_nodes, running_node_procs, debug=False):
    """
    Check and update status of currently running nodes.
    
    Args:
        conn: Database connection
        cur: Database cursor
        running_nodes: Set of currently running nodes
        completed_nodes: Set of completed nodes
        failed_nodes: Set of failed nodes
        running_node_procs: Dict mapping node_id to process/job_id
        debug: Enable debug logging
    """
    for node_id in list(running_nodes):
        try:
            if node_id in running_node_procs:
                status = check_proc_status(running_node_procs[node_id])
                db_utils.update_node_status(cur, node_id, status)
                conn.commit()
                
                if status == "completed":
                    completed_nodes.add(node_id)
                    running_nodes.discard(node_id)
                    running_node_procs.pop(node_id, None)
                elif status == "failed":
                    failed_nodes.add(node_id)
                    running_nodes.discard(node_id)
                    running_node_procs.pop(node_id, None)
        except Exception as e:
            if debug:
                print(f"Error checking status for node {node_id}: {e}")
            # Mark as failed if we can't check status
            failed_nodes.add(node_id)
            running_nodes.discard(node_id)
            running_node_procs.pop(node_id, None)
            try:
                db_utils.update_node_status(cur, node_id, "failed")
                conn.commit()
            except Exception:
                pass


def _find_runnable_nodes(cur, all_nodes, running_nodes, completed_nodes, failed_nodes, max_concurrent_nodes=None, debug=False):
    """
    Find nodes that can be run and are not already running/completed/failed.
    
    Args:
        cur: Database cursor
        all_nodes: Set of all nodes in pipeline
        running_nodes: Set of currently running nodes
        completed_nodes: Set of completed nodes
        failed_nodes: Set of failed nodes
        max_concurrent_nodes: Maximum number of nodes to run concurrently
        debug: Enable debug logging
    
    Returns:
        list: List of node IDs that can be run
    """
    try:
        runnable_nodes = [
            node_id for node_id in all_nodes
            if pip_utils.can_node_run(cur, node_id) and node_id not in (running_nodes | completed_nodes | failed_nodes)
        ]
    except Exception as e:
        if debug:
            print(f"Error finding runnable nodes: {e}")
        runnable_nodes = []

    # Apply concurrency limit
    if max_concurrent_nodes:
        available_slots = max_concurrent_nodes - len(running_nodes)
        runnable_nodes = runnable_nodes[:max(0, available_slots)]
    
    return runnable_nodes


def _check_execution_complete(all_nodes, completed_nodes, failed_nodes, runnable_nodes, running_nodes, debug=False):
    """
    Check if pipeline execution is complete.
    
    Args:
        all_nodes: Set of all nodes in pipeline
        completed_nodes: Set of completed nodes
        failed_nodes: Set of failed nodes
        runnable_nodes: List of nodes that can be run
        running_nodes: Set of currently running nodes
        debug: Enable debug logging
    
    Returns:
        bool: True if execution is complete
    """
    # Stop if all nodes are completed or failed
    if len(completed_nodes | failed_nodes) == len(all_nodes):
        if debug:
            print("Pipeline execution completed - all nodes processed")
        return True
    
    # Stop if no more can be scheduled and none running
    if not runnable_nodes and not running_nodes:
        if debug:
            print("Pipeline execution stopped - no runnable nodes and none running")
        return True
    
    return False


def _start_runnable_nodes(conn, cur, runnable_nodes, running_nodes, failed_nodes, running_node_procs, debug=False):
    """
    Start processes for runnable nodes.
    
    Args:
        conn: Database connection
        cur: Database cursor
        runnable_nodes: List of nodes that can be run
        running_nodes: Set of currently running nodes
        failed_nodes: Set of failed nodes
        running_node_procs: Dict mapping node_id to process/job_id
        debug: Enable debug logging
    """
    for node_id in runnable_nodes:
        try:
            running_node_procs[node_id] = submit_run_node(conn, node_id)
            running_nodes.add(node_id)
            if debug:
                print(f"Started running node {node_id}")

        except RuntimeError as e:
            if debug:
                print(f"Error running node {node_id}: {e}")
            failed_nodes.add(node_id)
            try:
                db_utils.update_node_status(cur, node_id, "failed")
                conn.commit()
            except Exception as commit_error:
                if debug:
                    print(f"Error updating failed status for node {node_id}: {commit_error}")
                conn.rollback()


def _handle_timeout_cleanup(conn, running_nodes, failed_nodes, running_node_procs, timeout, debug=False):
    """
    Handle timeout by killing all running processes.
    
    Args:
        conn: Database connection
        running_nodes: Set of currently running nodes
        failed_nodes: Set of failed nodes
        running_node_procs: Dict mapping node_id to process/job_id
        timeout: Timeout value for error message
        debug: Enable debug logging
    
    Raises:
        TimeoutError: Always raised after cleanup
    """
    for node_id in list(running_nodes):
        try:
            kill_running_process(conn, node_id)
            failed_nodes.add(node_id)
            running_nodes.discard(node_id)
            running_node_procs.pop(node_id, None)
        except Exception as e:
            if debug:
                print(f"Error killing node {node_id} on timeout: {e}")
    
    raise TimeoutError(f"Pipeline execution exceeded timeout of {timeout} seconds")


def _handle_interrupt_cleanup(conn, running_nodes, failed_nodes, running_node_procs, debug=False):
    """
    Handle interrupt by killing all running processes.
    
    Args:
        conn: Database connection
        running_nodes: Set of currently running nodes
        failed_nodes: Set of failed nodes
        running_node_procs: Dict mapping node_id to process/job_id
        debug: Enable debug logging
    """
    if debug:
        print("Pipeline execution interrupted - cleaning up running processes")
    
    for node_id in list(running_nodes):
        try:
            kill_running_process(conn, node_id)
            failed_nodes.add(node_id)
            running_nodes.discard(node_id)
            running_node_procs.pop(node_id, None)
        except Exception as e:
            if debug:
                print(f"Error killing node {node_id} during cleanup: {e}")


def _create_execution_summary(completed_nodes, failed_nodes, excluded_nodes, all_nodes, execution_time):
    """
    Create execution summary dictionary.
    
    Args:
        completed_nodes: Set of completed nodes
        failed_nodes: Set of failed nodes
        excluded_nodes: Set of excluded nodes
        all_nodes: Set of all nodes that were supposed to run
        execution_time: Total execution time in seconds
    
    Returns:
        dict: Execution summary
    """
    return {
        "status": "completed" if len(failed_nodes) == 0 else "failed",
        "completed": len(completed_nodes),
        "failed": len(failed_nodes),
        "skipped": len(excluded_nodes),
        "total": len(all_nodes) + len(excluded_nodes),
        "execution_time": execution_time
    }