# This script provides example on how to run a python script, matlab script, python notebook
# Warning you must set up the following enviroment variable to get access to the database:
# export DATABASE_URL="dbname=<yourdb> user=<youruser> password=<yourpassword> host=localhost port=<port>"
import os
from datetime import datetime

if __name__ == "__main__":
    # -- Example run simple python functions --

    # Import example functions
    from example_python import print_node_parents, save_dummy_output
    print("Running python example from function")
    print_node_parents()
    save_dummy_output()

    # -- Example run matlab script --
    # Uncomment the following lines to run the matlab example
    # print("Running matlab script")
    # from subprocess import run
    # run(["/usr/local/matlab-25.1/bin/matlab","-batch","example_matlab"], check=True, cwd=os.path.dirname(__file__), env=os.environ.copy())

    # -- Example run python notebook --
    # Uncomment the following lines to run the notebook example
    # print("Running python notebook")
    # # This will save the executed notebook into a different file.
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # run([
    #     "jupyter", "nbconvert", "--to", "notebook", "--execute", "example_notebook.ipynb",
    #     "--output", f"executed_example_notebook_{timestamp}.ipynb",
    #     "--ExecutePreprocessor.kernel_name=python3" # Usually you will have a kernel with the same name of the node
    # ], check=True, cwd=os.path.dirname(__file__), env=os.environ.copy())