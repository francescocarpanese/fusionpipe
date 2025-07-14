examples = {
    "example_python": {
        "header": "--- Example Python Functions ---",
        "code":  """
    print("Running python functions")
    from examples.example_python import print_node_parents, save_dummy_output
    print("Running python example from function")
    print_node_parents()
    save_dummy_output()
    """,
    },
    "example_matlab": {
        "header": "--- Example run matlab script ---",
        "code": """
    print("Running matlab script")
    from subprocess import run
    script_dir = os.path.join(os.path.dirname(__file__), "examples")
    matlab_path = os.environ.get("FP_MATLAB_RUNNER_PATH", "/usr/local/matlab-25.1/bin/matlab")
    run([matlab_path, "-batch", "example_matlab"], check=True, cwd=script_dir, env=os.environ.copy())
    """,
    },
    "example_notebook": {
        "header": "--- Example run python notebook ---",
        "code": """
    print("Running python notebook")
    from subprocess import run
    kernel_name = get_node_id() # This is the name of the kernel that you have been using during developement of this node. Usually the same is the same as the node_id
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run([
        "uv", "run", "jupyter", "nbconvert", "--to", "notebook", "--execute", "examples/example_notebook.ipynb",
        "--output", f"executed_example_notebook_{timestamp}.ipynb",
        f"--ExecutePreprocessor.kernel_name={kernel_name}"
    ], check=True, cwd=os.path.dirname(__file__), env=os.environ.copy())
    """,
    },
    "example_python_ray": {
        "header": "--- Example Python Functions ---",
        "code":  """
    print("Running python functions")
    from examples.example_python import run_ray_example
    print("Running python example from function")
    run_ray_example()
    """,
    },
}

def code_snippet_to_commented(code):
    """Convert code snippet to a commented string."""
    return "\n".join(f"# {line}" for line in code.strip().splitlines())

def generate_commented_examples_for_main():
    """Generate properly formatted commented examples for appending to main.py."""
    example_comments = []
    for example_name, example_data in examples.items():
        example_comments.append(f"\n    # {example_data['header']}")
        # Process each line to maintain proper indentation when commenting
        code_lines = example_data['code'].strip().splitlines()
        for line in code_lines:
            if line.strip():  # Only process non-empty lines
                # Remove leading spaces and add proper comment indentation
                clean_line = line.lstrip()
                example_comments.append(f"    # {clean_line}")
            else:
                example_comments.append("    #")
    
    # Return the formatted examples as a single string
    return "\n".join(example_comments)