import os
import sys

from fp_user_utils.user_api import get_current_node_id, get_info_parents, get_current_node_folder_path, get_current_node_folder_path_data

def print_node_parents():
    print("Template fusionpipe")
    # Put here the id of the node, or fetch from the name of the folder
    node_id = get_current_node_id()
    # Get the parent folder where you can read the outputs of the parent nodes
    info_parents = get_info_parents(node_id=node_id)
    print(f"Parent folders for node {node_id}:\n")
    for parent in info_parents:
        print(parent)
        print('\n')

def save_dummy_output():
    data_folder_path = get_current_node_folder_path()
    output_path = os.path.join(get_current_node_folder_path_data(), 'dummy.txt') # Get the data folder path for the current node
    with open(output_path, "w") as f:
        f.write("This is a dummy output file for the fusionpipe template.\n")
    print(f"Dummy output saved to {output_path}")

def run_ray_example():
    """
    Run a Ray example to demonstrate distributed processing on a dataset.
    This function initializes Ray and applies a remote operation to each item in a dummy dataset in parallel.

    Explanation:
    - A remote function `process_item` is defined, which simulates processing a dataset item.
    - The dummy dataset is a list of dictionaries.
    - Each item is processed in parallel using Ray.
    - The results are collected and printed.
    """

    import ray
    ray.init()

    # Simulate a dataset: list of dicts
    dataset = [{"id": i, "value": i * 10} for i in range(10)]

    @ray.remote
    def process_item(item):
        # Simulate a computation (e.g., multiply value by 2)
        return {"id": item["id"], "processed_value": item["value"] * 2}

    # Launch jobs in parallel for each item in the dataset
    # To fine tuning your resource allocation visit https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote.html#ray.remote
    futures = [process_item.remote(item) for item in dataset]
    results = ray.get(futures)
    print("Processed dataset:", results)