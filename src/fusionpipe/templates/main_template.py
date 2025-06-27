# This script provides example on how to run a python script, matlab script, python notebook
# Warning you must set up the following enviroment variable to get access to the database:
# export DATABASE_URL="dbname=<yourdb> user=<youruser> password=<yourpassword> host=localhost port=<port>"
import os
from datetime import datetime
import sys
sys.path.insert(0, os.environ.get("USER_UTILS_FOLDER_PATH"))
from python_user_utils.node_api import get_all_parent_node_folder_paths, get_node_id, get_folder_path_data, get_folder_path_reports, get_folder_path_code, get_folder_path_node


if __name__ == "__main__":
    print("Running main template.py")
    # -- Example run simple python functions --
    # Import example functions
    # from examples.example_python import print_node_parents, save_dummy_output
    # print("Running python example from function")
    # print_node_parents()
    # save_dummy_output()

    # -- Example run matlab script --
    # Uncomment the following lines to run the matlab example
    # print("Running matlab script")
    # from subprocess import run
    # script_dir = os.path.join(os.path.dirname(__file__), "examples") # Matlab is set to run from the folder of the script
    # run(["/usr/local/matlab-25.1/bin/matlab","-batch","example_matlab"], check=True, cwd=script_dir, env=os.environ.copy())

    # -- Example run python notebook --
    ## Uncomment the following lines to run the notebook example
    # print("Running python notebook")
    # # This will save the executed notebook into a different file.
    # from subprocess import run
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # kernel_name = get_node_id() # This is the name of the kernel that you have been using during developement of this node. Usually the same is the same as the node_id
    # run([
    #     "uv", "run", "jupyter", "nbconvert", "--to", "notebook", "--execute", "examples/example_notebook.ipynb",
    #     "--output", f"executed_example_notebook_{timestamp}.ipynb",
    #     f"--ExecutePreprocessor.kernel_name={kernel_name}"  # Use the kernel_name variable
    # ], check=True, cwd=os.path.dirname(__file__), env=os.environ.copy())