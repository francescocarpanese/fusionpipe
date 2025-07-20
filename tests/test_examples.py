import pytest
import os
import yaml
from unittest.mock import patch, MagicMock
from fusionpipe.utils import db_utils, pip_utils, runner_utils
from fusionpipe.utils.example_generator import examples


@pytest.mark.parametrize("example_name", list(examples.keys()))
def test_example_execution(pg_test_db, tmp_base_dir, example_name):
    """
    Test each example from the example generator by:
    1. Creating and initializing a new node
    2. Adding the uncommented example text to main.py
    3. Running the node with run_node function
    """
    from fusionpipe.utils import db_utils, pip_utils, runner_utils

    # Setup DB and pipeline
    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag=f"test_example_{example_name}")

    # Create a new node for this example
    node_id = pip_utils.generate_node_id()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    
    # Add node to database
    db_utils.add_node_to_nodes(
        cur, 
        node_id=node_id, 
        editable=True, 
        folder_path=folder_path_nodes, 
        status="ready"
    )
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    
    # Initialize node folder structure
    pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
    
    # Get the example code
    example_data = examples[example_name]
    example_code = example_data["code"].strip()
    
    # Read the current main.py template
    main_file_path = os.path.join(folder_path_nodes, "code", "main.py")
    with open(main_file_path, "r") as f:
        main_content = f.read()
    
    # Add the example code to main.py (uncommented)
    # Insert the example code before the final line
    lines = main_content.split('\n')
    
    # Find the insertion point (before the last line or at the end)
    insertion_point = len(lines)
    for i, line in enumerate(lines):
        if line.strip() == 'print("Running main template.py")':
            insertion_point = i
            break
    
    # Add example header and code
    example_lines = [
        f"    # {example_data['header']}",
        ""
    ]
    
    # Add the actual code lines with proper indentation
    for line in example_code.split('\n'):
        if line.strip():
            # Remove any existing indentation and add 4 spaces
            clean_line = line.lstrip()
            example_lines.append(f"    {clean_line}")
        else:
            example_lines.append("    ")
    
    example_lines.append("")  # Add blank line after example
    
    # Insert the example code
    lines[insertion_point:insertion_point] = example_lines
    
    # Write the modified main.py
    with open(main_file_path, "w") as f:
        f.write('\n'.join(lines))
    
    # Set run mode to local (to avoid Ray dependency issues in tests)
    param_file = os.path.join(folder_path_nodes, "code", "node_parameters.yaml")
    with open(param_file, "r") as f:
        params = yaml.safe_load(f)
    params["run_mode"] = "local"
    with open(param_file, "w") as f:
        yaml.safe_dump(params, f)
    
    conn.commit()

    # Get the test database URL to pass to subprocess
    # Build the database URL from connection parameters in format dbname=... host=... port=... user=...
    test_db_params = conn.get_dsn_parameters()
    test_db_url_parts = []
    
    for key in ['dbname', 'host', 'port', 'user', 'password']:
        if key in test_db_params and test_db_params[key]:
            test_db_url_parts.append(f"{key}={test_db_params[key]}")
    
    test_db_url = " ".join(test_db_url_parts)

    # Allow real execution but handle potential failures gracefully
    # Only mock Ray cluster initialization to avoid external dependency
    # Set DATABASE_URL to the test database so subprocess can connect to the same DB
    with patch.dict(os.environ, {
        "FUSIONPIPE_DATA_PATH": tmp_base_dir,
        "DATABASE_URL": test_db_url,
    }), \
         patch('fusionpipe.utils.runner_utils.init_ray_cluster') as mock_ray_init:
        
        try:
            # Run the node with real execution
            runner_utils.run_node(conn, node_id)
            execution_success = True
            error_message = None
        except Exception as e:
            execution_success = False
            error_message = str(e)
            print(f"Example {example_name} failed with error: {error_message}")

    # Check final node status
    final_status = db_utils.get_node_status(cur, node_id)
    
    # Determine if execution was actually successful based on final status
    # The run_node function doesn't raise exceptions for process failures,
    # it just updates the database status
    execution_actually_succeeded = (final_status == "completed")
    
    if not execution_actually_succeeded and execution_success:
        # Process failed but run_node didn't raise an exception
        # Let's try to get the actual error from log file
        log_file_path = os.path.join(folder_path_nodes, "logs.txt")
        if os.path.exists(log_file_path):
            with open(log_file_path, "r") as f:
                log_content = f.read()
            error_message = f"Process failed, log content: {log_content[-500:]}"  # Last 500 chars
        else:
            error_message = "Process failed but no log file found"
        print(f"Example {example_name} failed with error: {error_message}")
    
    # Now we can test actual execution behavior
    assert final_status == "completed", f"Example {example_name} should complete successfully when execution succeeds"
    print(f"✓ Example {example_name} executed successfully")


    conn.close()


def test_all_examples_are_valid():
    """Test that all examples in the example generator are properly formatted"""
    
    # Check that examples dictionary is not empty
    assert len(examples) > 0, "Examples dictionary should not be empty"
    
    # Check that each example has required fields
    for example_name, example_data in examples.items():
        assert "header" in example_data, f"Example {example_name} missing 'header' field"
        assert "code" in example_data, f"Example {example_name} missing 'code' field"
        
        # Check that header is a string
        assert isinstance(example_data["header"], str), f"Example {example_name} header should be a string"
        
        # Check that code is a string
        assert isinstance(example_data["code"], str), f"Example {example_name} code should be a string"
        
        # Check that code is not empty
        assert example_data["code"].strip(), f"Example {example_name} code should not be empty"
        
        # Check that header starts with expected format
        assert example_data["header"].startswith("--- Example"), f"Example {example_name} header should start with '--- Example'"



def test_example_code_injection(tmp_base_dir):
    """Test that example code can be properly injected into main.py"""
    
    # Create a temporary main.py file based on the template
    main_template_content = '''# This script provides example on how to run a python script, matlab script, python notebook
# Warning you must set up the following enviroment variable to get access to the database unless not already set in your .bash_profile:
# export DATABASE_URL="dbname=<yourdb> port=<port>" 
import os
from datetime import datetime
import sys
from fp_user_utils.user_api import get_current_node_id, get_info_parents


if __name__ == "__main__":
    print("Running main template.py")
'''
    
    # Create temporary file
    temp_main_path = os.path.join(tmp_base_dir, "test_main.py")
    with open(temp_main_path, "w") as f:
        f.write(main_template_content)
    
    # Test injection for each example
    for example_name, example_data in examples.items():
        # Reset file content
        with open(temp_main_path, "w") as f:
            f.write(main_template_content)
        
        # Read current content
        with open(temp_main_path, "r") as f:
            content = f.read()
        
        lines = content.split('\n')
        
        # Find insertion point
        insertion_point = len(lines)
        for i, line in enumerate(lines):
            if line.strip() == 'print("Running main template.py")':
                insertion_point = i
                break
        
        # Prepare example code
        example_code = example_data["code"].strip()
        example_lines = [
            f"    # {example_data['header']}",
            ""
        ]
        
        for line in example_code.split('\n'):
            if line.strip():
                clean_line = line.lstrip()
                example_lines.append(f"    {clean_line}")
            else:
                example_lines.append("    ")
        
        example_lines.append("")
        
        # Insert the example code
        lines[insertion_point:insertion_point] = example_lines
        
        # Write modified content
        with open(temp_main_path, "w") as f:
            f.write('\n'.join(lines))
        
        # Verify injection worked
        with open(temp_main_path, "r") as f:
            modified_content = f.read()
        
        assert example_data['header'] in modified_content, f"Header not found for {example_name}"
        
        # Verify that the code was uncommented
        if example_name == "example_python":
            assert "from examples.example_python import" in modified_content, "Python import not uncommented"
        elif example_name == "example_matlab":
            assert "matlab_path = os.environ.get" in modified_content, "MATLAB path not uncommented"
        elif example_name == "example_notebook":
            assert "jupyter" in modified_content, "Jupyter reference not uncommented"
    
    # Clean up
    if os.path.exists(temp_main_path):
        os.remove(temp_main_path)
