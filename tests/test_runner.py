import pytest
import tempfile
import os
import sqlite3
import time
import subprocess
import yaml
from unittest.mock import patch, MagicMock, mock_open
from fusionpipe.utils import db_utils, pip_utils, runner_utils

@pytest.mark.parametrize("last_node,expected_status_a,expected_status_b,expected_status_c", [
    (None, "completed", "completed", "completed"),
    (0, "completed", "ready", "ready"),
    (1, "completed", "completed", "ready"),
    (2, "completed", "completed", "completed"),
])
def test_run_pipeline(pg_test_db, tmp_path, last_node, expected_status_a, expected_status_b, expected_status_c):
    """
    Test the runner_utils.run_pipeline function for different starting nodes.
    Verifies that pipeline execution updates node statuses as expected:
    - If last_node is None, the pipeline runs from the beginning.
    - If last_node is specified, the pipeline runs starting from that node.
    Checks that each node's status matches the expected value after execution.
    """
    from fusionpipe.utils import db_utils, pip_utils, runner_utils

    # Setup DB and pipeline
    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Create three nodes (A -> B -> C)
    node_a = pip_utils.generate_node_id()
    node_b = pip_utils.generate_node_id()
    node_c = pip_utils.generate_node_id()
    node_ids = [node_a, node_b, node_c]
    folder_a = os.path.join(tmp_path, node_a)
    folder_b = os.path.join(tmp_path, node_b)
    folder_c = os.path.join(tmp_path, node_c)

    for node_id, folder in zip(node_ids, [folder_a, folder_b, folder_c]):
        db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", editable=True, folder_path=folder)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
        pip_utils.init_node_folder(folder_path_nodes=folder)

    db_utils.add_node_relation(cur, child_id=node_b, parent_id=node_a)
    db_utils.add_node_relation(cur, child_id=node_c, parent_id=node_b)
    conn.commit()

    # Run pipeline from specified node or from the beginning
    if last_node is not None:
        runner_utils.run_pipeline(conn, pipeline_id, last_node_id=node_ids[last_node], poll_interval=0.2, debug=True)
    else:
        runner_utils.run_pipeline(conn, pipeline_id, poll_interval=0.2, debug=True)

    # Assert node statuses
    assert db_utils.get_node_status(cur, node_a) == expected_status_a
    assert db_utils.get_node_status(cur, node_b) == expected_status_b
    assert db_utils.get_node_status(cur, node_c) == expected_status_c

    conn.close()



@pytest.mark.parametrize("address,object_store_memory,temp_dir,num_cpus,should_fail", [
    (None, None, None, None, False),  # Default initialization
    ("localhost:10001", None, None, None, False),  # Connect to existing cluster
    (None, 1024*1024*1024, "/tmp/ray_test", 2, False),  # Custom config
])
def test_init_ray_cluster(address, object_store_memory, temp_dir, num_cpus, should_fail):
    """Test Ray cluster initialization with various configurations"""
    
    with patch('ray.is_initialized') as mock_is_initialized, \
         patch('ray.init') as mock_init, \
         patch('os.makedirs') as mock_makedirs:
        
        # Mock Ray not being initialized initially
        mock_is_initialized.return_value = False
        
        if should_fail:
            # Mock Ray init to raise an exception for invalid configurations
            mock_init.side_effect = Exception("Failed to connect to Ray cluster")
            
            with pytest.raises(Exception, match="Failed to initialize Ray"):
                runner_utils.init_ray_cluster(
                    address=address,
                    object_store_memory=object_store_memory,
                    temp_dir=temp_dir,
                    num_cpus=num_cpus
                )
        else:
            # Mock successful Ray initialization
            mock_init.return_value = None
            
            # Call the function
            runner_utils.init_ray_cluster(
                address=address,
                object_store_memory=object_store_memory,
                temp_dir=temp_dir,
                num_cpus=num_cpus
            )
            
            # Verify ray.init was called
            mock_init.assert_called_once()
            
            # Build expected kwargs
            expected_kwargs = {}
            if address:
                expected_kwargs['address'] = address
            if object_store_memory:
                expected_kwargs['object_store_memory'] = object_store_memory
            if temp_dir:
                expected_kwargs['temp_dir'] = temp_dir
            if num_cpus:
                expected_kwargs['num_cpus'] = num_cpus
            
            # Verify the arguments passed to ray.init
            actual_kwargs = mock_init.call_args[1]
            for key, value in expected_kwargs.items():
                assert actual_kwargs[key] == value
            
            # Verify temp directory creation if specified
            if temp_dir:
                mock_makedirs.assert_called_once_with(temp_dir, exist_ok=True)


@pytest.mark.parametrize("node_init_status,expected_status,run_mode", [
    ("ready", "completed","ray"),
    ("running", "running","ray"),
    ("failed", "failed","ray"),
    ("completed", "completed","ray"),
    ("ready", "completed","local"),
    ("running", "running","local"),
    ("failed", "failed","local"),
    ("completed", "completed","local"),    
])
def test_create_and_run_node_from_parameter_file(pg_test_db, tmp_base_dir, node_init_status, expected_status, run_mode):
    """
    Test running a node by modifying the node_parameters.yaml file to set run_mode to "ray"
    and using the submit_run_node function which reads the run_mode from the parameter file.
    """
    import yaml
    from unittest.mock import patch

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status=node_init_status)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    
    # Modify the node_parameters.yaml file to set run_mode to "ray"
    param_file = os.path.join(folder_path_nodes, "code", "node_parameters.yaml")
    with open(param_file, "r") as f:
        params = yaml.safe_load(f)
    params["run_mode"] = run_mode
    with open(param_file, "w") as f:
        yaml.safe_dump(params, f)
    conn.commit()

    # Patch the environment variable so internal functions use the correct data path
    with patch.dict(os.environ, {"FUSIONPIPE_DATA_PATH": tmp_base_dir}):
        pass  # The rest of the test continues below

    # Check that no process exists for this node before running
    processes_before = db_utils.get_processes_by_node(cur, node_id)
    assert not processes_before, f"Expected no process for node {node_id} before running, found: {processes_before}"

    try:
        error_message = None
        runner_utils.run_node(conn,node_id)
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


@pytest.mark.parametrize("run_mode", ["local", "ray"])
def test_node_execution_context_success(pg_test_db, tmp_base_dir, run_mode):
    """Test successful execution of node_execution_context"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    proc_ref = None  # Initialize proc_ref before using

    with patch('ray.is_initialized', return_value=False), \
         patch.object(runner_utils, 'init_ray_cluster') as mock_init_ray:
        
        # Use the context manager
        with runner_utils.node_execution_context(conn, node_id, run_mode) as (cur, node_path, log_file, proc_ref):
            # Verify setup
            assert cur is not None
            assert node_path == folder_path_nodes
            assert log_file.endswith("logs.txt")
            assert proc_ref["proc"] is None
            
            # Check node status was set to running
            status = db_utils.get_node_status(cur, node_id)
            assert status == "running"
            
            # Verify Ray initialization was called if run_mode is ray
            if run_mode == "ray":
                mock_init_ray.assert_called_once()
            else:
                mock_init_ray.assert_not_called()

    # After context manager, node should still be running (no error occurred)
    final_status = db_utils.get_node_status(cur, node_id)
    assert final_status == "running"

    conn.close()


def test_node_execution_context_node_cannot_run(pg_test_db, tmp_base_dir):
    """Test context manager when node cannot run"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node that cannot run
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    # Don't initialize node folder to make it unrunnable
    conn.commit()

    with patch.object(pip_utils, 'can_node_run', return_value=False):
        with pytest.raises(RuntimeError, match=f"Node {node_id} cannot be run"):
            with runner_utils.node_execution_context(conn, node_id, "local") as context:
                pass

    conn.close()


def test_node_execution_context_exception_cleanup(pg_test_db, tmp_base_dir):
    """Test context manager cleanup when exception occurs inside context"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    # Mock process for cleanup testing
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    log_file = os.path.join(folder_path_nodes, "logs.txt")
    proc_ref = {"proc": mock_proc}  # Initialize proc_ref with mock process
    node_path = folder_path_nodes

    with pytest.raises(ValueError, match="Test exception"):
        with runner_utils.node_execution_context(conn, node_id, "local") as (cur, node_path, log_file, proc_ref):
            # Simulate process creation
            proc_ref["proc"] = mock_proc
            
            # Verify node is running
            status = db_utils.get_node_status(cur, node_id)
            assert status == "running"
            
            # Raise an exception to test cleanup
            raise ValueError("Test exception")

    # Verify cleanup occurred
    final_status = db_utils.get_node_status(cur, node_id)
    assert final_status == "failed"

    conn.close()



def test_create_ray_job(pg_test_db, tmp_base_dir):
    """Test _create_ray_job function"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    log_file = os.path.join(folder_path_nodes, "logs.txt")
    test_job_id = "test_job_12345"
    
    # Mock Ray JobSubmissionClient
    mock_client = MagicMock()
    mock_client.submit_job.return_value = test_job_id
    
    with patch('fusionpipe.utils.runner_utils.JobSubmissionClient', return_value=mock_client), \
         patch.dict(os.environ, {'RAY_SUBMIT_URL': 'http://localhost:8265'}), \
         patch('builtins.open', mock_open()) as mock_file:
        
        # Call the function
        result_job_id = runner_utils._create_ray_job(cur, node_id, folder_path_nodes, log_file)
        
        # Verify JobSubmissionClient was created with correct URL
        runner_utils.JobSubmissionClient.assert_called_once_with('http://localhost:8265')
        
        # Verify submit_job was called correctly
        mock_client.submit_job.assert_called_once()
        call_kwargs = mock_client.submit_job.call_args[1]
        assert call_kwargs['entrypoint'] == "uv run main.py"
        assert call_kwargs['runtime_env']['working_dir'] == os.path.join(folder_path_nodes, "code")
        assert call_kwargs['submission_id'].startswith(f"ray_{node_id}_")
        
        # Verify the job was added to database
        processes = db_utils.get_processes_by_node(cur, node_id)
        assert len(processes) == 1
        assert processes[0]['process_id'] == test_job_id
        assert processes[0]['status'] == "running"
        
        # Verify return value
        assert result_job_id == test_job_id

    conn.close()


def test_create_local_process_file_operations(pg_test_db, tmp_base_dir):
    """Test _create_local_process file writing and logging"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    log_file = os.path.join(folder_path_nodes, "logs.txt")
    
    # Mock subprocess.Popen
    mock_proc = MagicMock()
    mock_proc.pid = 54321
    mock_proc.poll.return_value = None  # Simulate running process

    # Create actual log file for testing
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    with patch('subprocess.Popen', return_value=mock_proc):
        # Call the function
        result_proc = runner_utils._create_local_process(cur, node_id, folder_path_nodes, log_file)
        
        # Verify log file was written
        assert os.path.exists(log_file)
        with open(log_file, 'r') as f:
            log_content = f.read()
            assert f"PID: {mock_proc.pid}" in log_content
            assert "---" in log_content

    conn.close()


def test_create_ray_job_submission_id_format(pg_test_db, tmp_base_dir):
    """Test _create_ray_job submission ID format"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    log_file = os.path.join(folder_path_nodes, "logs.txt")
    test_job_id = "ray_job_response_12345"
    
    # Mock Ray JobSubmissionClient
    mock_client = MagicMock()
    mock_client.submit_job.return_value = test_job_id
    
    with patch('fusionpipe.utils.runner_utils.JobSubmissionClient', return_value=mock_client), \
         patch.dict(os.environ, {'RAY_SUBMIT_URL': 'http://localhost:8265'}), \
         patch('time.strftime', return_value='20240101_120000'):
        
        # Call the function
        result_job_id = runner_utils._create_ray_job(cur, node_id, folder_path_nodes, log_file)
        
        # Verify submission_id format
        call_kwargs = mock_client.submit_job.call_args[1]
        expected_submission_id = f"ray_{node_id}_20240101_120000"
        assert call_kwargs['submission_id'] == expected_submission_id

    conn.close()



def test_create_ray_job_client_error_handling(pg_test_db, tmp_base_dir):
    """Test _create_ray_job error handling when Ray client fails"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    log_file = os.path.join(folder_path_nodes, "logs.txt")
    
    # Mock Ray JobSubmissionClient to raise an exception
    with patch('fusionpipe.utils.runner_utils.JobSubmissionClient', side_effect=Exception("Ray connection failed")), \
         patch.dict(os.environ, {'RAY_SUBMIT_URL': 'http://localhost:8265'}):
        
        with pytest.raises(Exception, match="Ray connection failed"):
            runner_utils._create_ray_job(cur, node_id, folder_path_nodes, log_file)

    conn.close()


def test_write_execution_start_log(tmp_base_dir):
    """Test _write_execution_start_log function"""
    log_file = os.path.join(tmp_base_dir, "test_logs.txt")
    node_id = "test_node_123"
    run_mode = "local"
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    with patch('time.strftime', return_value='2024-01-01 12:00:00'):
        runner_utils._write_execution_start_log(log_file, node_id, run_mode)
    
    # Verify log file content
    assert os.path.exists(log_file)
    with open(log_file, 'r') as f:
        content = f.read()
        assert f"Node {node_id} execution starting" in content
        assert "Time: 2024-01-01 12:00:00" in content
        assert f"Run mode: {run_mode}" in content
        assert "---" in content


def test_submit_node_with_run_mode_local_submission_failure(pg_test_db, tmp_base_dir):
    """Test submission failure handling for local processes"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    # Mock subprocess.Popen to fail
    with patch('subprocess.Popen', side_effect=subprocess.SubprocessError("Process creation failed")):
        with pytest.raises(RuntimeError, match="Submission failed for node"):
            runner_utils.submit_node_with_run_mode(conn, node_id, run_mode="local")
    
    # Verify node status was set to failed
    final_status = db_utils.get_node_status(cur, node_id)
    assert final_status == "failed"
    
    # Verify no process was added to database
    processes = db_utils.get_processes_by_node(cur, node_id)
    assert len(processes) == 0

    conn.close()


def test_submit_node_with_run_mode_ray_submission_failure(pg_test_db, tmp_base_dir):
    """Test submission failure handling for Ray jobs"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    # Mock Ray JobSubmissionClient to fail
    mock_client = MagicMock()
    mock_client.submit_job.side_effect = Exception("Ray cluster unavailable")
    
    with patch('fusionpipe.utils.runner_utils.JobSubmissionClient', return_value=mock_client), \
         patch.dict(os.environ, {'RAY_SUBMIT_URL': 'http://localhost:8265'}):
        
        with pytest.raises(RuntimeError, match="Submission failed for node"):
            runner_utils.submit_node_with_run_mode(conn, node_id, run_mode="ray")
    
    # Verify node status was set to failed
    final_status = db_utils.get_node_status(cur, node_id)
    assert final_status == "failed"
    
    # Verify no process was added to database
    processes = db_utils.get_processes_by_node(cur, node_id)
    assert len(processes) == 0

    conn.close()


def test_submit_node_with_run_mode_process_dies_immediately(pg_test_db, tmp_base_dir):
    """Test handling when local process terminates immediately"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    # Mock process that terminates immediately
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.poll.return_value = 1  # Process terminated with error code 1
    mock_proc.returncode = 1
    
    with patch('subprocess.Popen', return_value=mock_proc):
        with pytest.raises(RuntimeError, match="Submission failed for node"):
            runner_utils.submit_node_with_run_mode(conn, node_id, run_mode="local")
    
    # Verify node status was set to failed
    final_status = db_utils.get_node_status(cur, node_id)
    assert final_status == "failed"

    conn.close()


def test_submit_node_with_run_mode_ray_job_not_found(pg_test_db, tmp_base_dir):
    """Test handling when Ray job is not found after submission"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    # Mock Ray client that returns job ID but job is not found when checked
    mock_client = MagicMock()
    mock_client.submit_job.return_value = "test_job_123"
    mock_client.get_job_status.return_value = None  # Job not found
    
    with patch('fusionpipe.utils.runner_utils.JobSubmissionClient', return_value=mock_client), \
         patch.dict(os.environ, {'RAY_SUBMIT_URL': 'http://localhost:8265'}):
        
        with pytest.raises(RuntimeError, match="Submission failed for node"):
            runner_utils.submit_node_with_run_mode(conn, node_id, run_mode="ray")
    
    # Verify node status was set to failed
    final_status = db_utils.get_node_status(cur, node_id)
    assert final_status == "failed"

    conn.close()


def test_submit_node_with_run_mode_missing_main_py(pg_test_db, tmp_base_dir):
    """Test handling when main.py is missing"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    
    # Remove main.py to simulate missing file
    main_py_path = os.path.join(folder_path_nodes, "code", "main.py")
    if os.path.exists(main_py_path):
        os.remove(main_py_path)
    
    conn.commit()

    # Test both local and ray modes
    for run_mode in ["local", "ray"]:
        with pytest.raises(RuntimeError, match="Submission failed for node"):
            runner_utils.submit_node_with_run_mode(conn, node_id, run_mode=run_mode)
        
        # Verify node status was set to failed
        final_status = db_utils.get_node_status(cur, node_id)
        assert final_status == "failed"
        
        # Reset node status for next test
        db_utils.update_node_status(cur, node_id, "ready")
        conn.commit()

    conn.close()


def test_submit_node_with_run_mode_missing_ray_submit_url(pg_test_db, tmp_base_dir):
    """Test handling when RAY_SUBMIT_URL is not set"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    # Remove RAY_SUBMIT_URL environment variable
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="Submission failed for node"):
            runner_utils.submit_node_with_run_mode(conn, node_id, run_mode="ray")
    
    # Verify node status was set to failed
    final_status = db_utils.get_node_status(cur, node_id)
    assert final_status == "failed"

    conn.close()


def test_submit_node_successful_submission_verification(pg_test_db, tmp_base_dir):
    """Test successful submission with proper verification"""
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Setup test node
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True, folder_path=folder_path_nodes, status="ready")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    conn.commit()

    # Test successful local submission
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.poll.return_value = None  # Process is running
    
    with patch('subprocess.Popen', return_value=mock_proc):
        result = runner_utils.submit_node_with_run_mode(conn, node_id, run_mode="local")
        assert result == mock_proc
        
        # Verify process was added to database
        processes = db_utils.get_processes_by_node(cur, node_id)
        assert len(processes) == 1
        assert processes[0]['process_id'] == str(mock_proc.pid)

    conn.close()