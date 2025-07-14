import os
import sys
# Add path to the user_utils module
sys.path.insert(0, os.environ.get("USER_UTILS_FOLDER_PATH"))

from python_user_utils.node_api import get_all_parent_node_folder_paths, get_node_id, get_folder_path_data, get_folder_path_reports

def print_node_parents():
    print("Template fusionpipe")
    # Put here the id of the node, or fetch from the name of the folder
    node_id = get_node_id()
    # Get the parent folder where you can read the outputs of the parent nodes
    parent_folders = get_all_parent_node_folder_paths(node_id=node_id)
    print(f"Parent folders for node {node_id}:\n")
    for folder in parent_folders:
        print(folder)
        print('\n')

def save_dummy_output():
    output_path = os.path.join(get_folder_path_data(), 'dummy.txt') # Get the data folder path for the current node
    with open(output_path, "w") as f:
        f.write("This is a dummy output file for the fusionpipe template.\n")
    print(f"Dummy output saved to {output_path}")

def run_ray_example():
    """ Run a simple Ray example to demonstrate distributed computing.
    This function initializes Ray and runs a simple remote function.
    It is useful for testing the Ray setup in the FusionPipe environment.
    """
    import ray
    ray.init(address=os.getenv("RAY_CLUSTER_ADDRESS"))

    @ray.remote
    def simple_job(x):
        return x * x

    # Run a simple job as remote job in the ray cluster
    result = ray.get(simple_job.remote(5))
    print("Result:", result)