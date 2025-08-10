import os
import sys

from fp_user_utils.user_api import (
    get_current_node_id,
    get_info_parents,
    get_current_node_folder_path,
    get_current_node_folder_path_data,
    get_parent_info_from_edge_id,
    get_parent_folder_path_from_edge_id
)

def print_node_parents():
    """
    Example of using api utils to retrieve info of parent node of the current node where this script is run.
    """
    print("Template fusionpipe")
    # Get the id of the current node here this script is being run
    node_id = get_current_node_id()

    # Get info of all node parents. Read the documentation get_info_parents.
    info_parents = get_info_parents(node_id=node_id)
    print(f"Retrieved information of the node parents: {info_parents}")

    # In order to retrive information from a given parent of the current node it is recommend to refer to the parent based on the edge_id
    # Get information of a parent node from edge_id. The resulting dictionary will contain:
    # {
    #     'node_id': <parent_node_id>,
    #     'node_tag': <parent_node_tag>,
    #     'folder_path': <parent_folder_path>,
    #     'edge_id': <edge_id>
    # }
    edge_id = '01' # Put here the edge_id of interest
    parent_info = get_parent_info_from_edge_id(edge_id)
    print(f"Retrieved information of the parent node (edge_id: {edge_id}): {parent_info}")

    # You can get directly the folder path of the parent node from the edge_id
    edge_id = '01'
    parent_folder = get_parent_folder_path_from_edge_id(edge_id)
    print(f"Retrieved folder path of the parent node (edge_id: {edge_id}): {parent_folder}")
 

def save_dummy_output():
    """
    Example retrieving the folder path of the current node and saving a dummy output file.
    This is an example on how you can save output data of your node.
    """
    node_folder_path = get_current_node_folder_path()
    output_path = os.path.join(get_current_node_folder_path_data(), 'dummy.txt') # Get the data folder path for the current node
    with open(output_path, "w") as f:
        f.write("This is a dummy output file for the fusionpipe template.\n")
    print(f"Dummy output saved to {output_path}")

def run_ray_example():
    """
    Run an example using Ray cluster for parallelisation of the workload. See https://docs.ray.io/en/latest/cluster/getting-started.html
    This function initializes Ray and applies a "remote" operation to each item of dataset in parallel.

    Explanation:
    - "remote" is a decorator that allows a function to be executed in a separate Ray worker.
    - A remote function `process_item` is defined, which simulates processing a dataset item.
    - The dummy dataset is a list of dictionaries.
    - Each item is processed in parallel.
    - The results are collected and printed.
    """

    import ray
    ray.init()

    # Simulate a dataset: list of dicts
    dataset = [{"id": i, "value": i * 10} for i in range(10)]

    @ray.remote
    def process_item(item):
        # Test import of user utils works
        print(get_current_node_id())
        # Simulate a computation (e.g., multiply value by 2)
        return {"id": item["id"], "processed_value": item["value"] * 2}

    # Launch jobs in parallel for each item in the dataset
    # To fine tuning your resource allocation visit https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote.html#ray.remote
    futures = [process_item.remote(item) for item in dataset]
    results = ray.get(futures)
    print("Processed dataset:", results)