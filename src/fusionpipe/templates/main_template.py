# This script provides example on how to run a python script, matlab script, python notebook
# Warning you must set up the following enviroment variable to get access to the database:
# export DATABASE_URL="dbname=<yourdb> user=<youruser> password=<yourpassword> host=localhost port=<port>"
import os


if __name__ == "__main__":
    print("Running python example from function")
    # Import example functions
    from example_python import print_node_parents, save_dummy_output
    # Run example function
    print_node_parents()
    save_dummy_output()

    print("Running matlab script")
    # Run matlab script as a subprocess
    from subprocess import run
    run(["/usr/local/matlab-25.1/bin/matlab","-batch","example_matlab"], check=True, cwd=os.path.dirname(__file__), env=os.environ.copy())