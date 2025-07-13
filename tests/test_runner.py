import pytest
import tempfile
import os
import sqlite3
import time
from unittest.mock import patch, MagicMock
from fusionpipe.utils import db_utils, pip_utils, runner_utils


@pytest.mark.parametrize("node_init_status,expected_status", [
    ("ready", "completed"),
    ("running", "running"),
    ("failed", "failed"),
    ("completed", "completed"),
])
def test_create_and_run_node(pg_test_db, tmp_base_dir, node_init_status, expected_status):

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

    try:
        proc = runner_utils.submit_run_node(conn, node_id, run_mode="local")
        runner_utils.wait_subprocess_completion(conn, node_id, proc)
        error_message = None
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


@pytest.mark.parametrize("node_init_status,expected_status", [
    ("ready", "completed"),
    ("running", "running"),
    ("failed", "failed"),
    ("completed", "completed"),
])
def test_create_and_run_node_ray(pg_test_db, tmp_base_dir, node_init_status, expected_status):

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

    try:
        proc = runner_utils.submit_node_with_run_mode(conn, node_id, run_mode="ray")
        runner_utils.wait_ray_job_completion(conn, node_id, proc)
        error_message = None
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


@pytest.mark.parametrize("address,object_store_memory,temp_dir,num_cpus,should_fail", [
    (None, None, None, None, False),  # Default initialization
    ("localhost:10001", None, None, None, False),  # Connect to existing cluster
    (None, 1024*1024*1024, "/tmp/ray_test", 2, False),  # Custom config
    ("invalid_address", None, None, None, True),  # Invalid address should fail
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


# def test_init_ray_cluster_already_initialized():
#     """Test that init_ray_cluster returns early when Ray is already initialized"""
    
#     with patch('ray.is_initialized') as mock_is_initialized, \
#          patch('ray.init') as mock_init:
        
#         # Mock Ray being already initialized
#         mock_is_initialized.return_value = True
        
#         # Call the function
#         runner_utils.init_ray_cluster()
        
#         # Verify ray.init was NOT called
#         mock_init.assert_not_called()


# def test_init_ray_cluster_with_env_variable():
#     """Test init_ray_cluster uses RAY_SUBMIT_URL environment variable as default address"""
    
#     test_url = "http://localhost:8265"
    
#     with patch.dict(os.environ, {'RAY_SUBMIT_URL': test_url}), \
#          patch('ray.is_initialized') as mock_is_initialized, \
#          patch('ray.init') as mock_init:
        
#         # Mock Ray not being initialized
#         mock_is_initialized.return_value = False
#         mock_init.return_value = None
        
#         # Call function without explicit address
#         runner_utils.init_ray_cluster()
        
#         # Verify ray.init was called with the environment variable address
#         mock_init.assert_called_once()
#         actual_kwargs = mock_init.call_args[1]
#         assert actual_kwargs['address'] == test_url


@pytest.mark.parametrize("node_init_status,expected_status", [
    ("ready", "completed"),
    ("running", "running"),
    ("failed", "failed"),
    ("completed", "completed"),
])
def test_create_and_run_node_with_ray_parameter(pg_test_db, tmp_base_dir, node_init_status, expected_status):
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
    params["run_mode"] = "ray"
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
        proc = runner_utils.submit_run_node(conn, node_id)
        runner_utils.wait_ray_job_completion(conn, node_id, proc)
        error_message = None
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

@pytest.mark.parametrize("node_init_status,expected_status", [
    ("ready", "completed"),
    ("running", "running"),
    ("failed", "failed"),
    ("completed", "completed"),
])
def test_create_and_run_node_with_ray(pg_test_db, tmp_base_dir, node_init_status, expected_status):
    """
    Test running a node by modifying the node_parameters.yaml file to set run_mode to "ray"
    and using the submit_run_node function which reads the run_mode from the parameter file.
    """
    import yaml

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

    try:
        proc = runner_utils.submit_node_with_run_mode(conn, node_id, run_mode="ray")
        runner_utils.wait_ray_job_completion(conn, node_id, proc)
        error_message = None
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