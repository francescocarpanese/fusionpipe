import os
from user_utils.python.node_api import get_all_parent_node_folder_paths, get_node_id

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
    output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data","dummy_output.txt")
    with open(output_path, "w") as f:
        f.write("This is a dummy output file for the fusionpipe template.\n")
    print(f"Dummy output saved to {output_path}")