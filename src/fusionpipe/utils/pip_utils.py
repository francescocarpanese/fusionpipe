from datetime import datetime
import random
import os
import networkx as nx
import matplotlib.pyplot as plt
import json
from fusionpipe.utils import pipeline_db


from enum import Enum

class NodeState(Enum):
    READY = "ready"       # Node is created but not yet processed
    RUNNING = "running"       # Node is currently being processed
    COMPLETED = "completed"   # Node has finished processing successfully
    FAILED = "failed"         # Node processing failed
    STALEDATA = "staledata"           # Node is outdated or no longer valid


def generate_id():
    # Get the current date and time
    now = datetime.now()
    # Format the identifier as YYYYMMDDHHMMSS + random 4-digit number
    unique_id = now.strftime("%Y%m%d%H%M%S") + "_" +  f"{random.randint(1000, 9999):04d}"
    return unique_id

def generate_node_id():
    return "n_" + generate_id()

def generate_pip_id():
    return "p_" + generate_id()

def init_node_folder(settings, node_id, verbose=False):
    # Create folder structure for new node

    pth_node_folder = settings["node_folder"]
    
    # Create the node folder path
    node_folder_path = f"{pth_node_folder}/{node_id}"
    
    # Create the main node folder
    os.makedirs(node_folder_path, exist_ok=True)
    
    # Create subfolders 'code' and 'data'
    os.makedirs(f"{node_folder_path}/code", exist_ok=True)
    os.makedirs(f"{node_folder_path}/data", exist_ok=True)
    os.makedirs(f"{node_folder_path}/reports", exist_ok=True)
    
    if verbose:
        print(f"Node folder created at: {node_folder_path}")

def delete_node_folder(settings, node_id, verbose=False):
    pth_node_folder = settings["node_folder"]
    
    # Construct the node folder path
    node_folder_path = f"{pth_node_folder}/n_{node_id}"
    
    # Remove the node folder and its contents
    if os.path.exists(node_folder_path):
        os.rmdir(node_folder_path)
        if verbose:
            print(f"Node folder deleted: {node_folder_path}")
    else:
        if verbose:
            print(f"Node folder does not exist: {node_folder_path}")







# TODO. Still convenient for debuggiong to generate the file from the database
# def create_pipeline_file(settings, pip_id, verbose=False):
#     pth_pipeline_folder = settings["pipeline_folder"]
    
#     # Create the pipeline file path
#     pipeline_file_path = f"{pth_pipeline_folder}/{pip_id}.json"

#     pip_template = generate_pipeline_template(name=pip_id)

#     # Write the pipeline template to a JSON file
#     with open(pipeline_file_path, 'w') as f:
#         import json
#         json.dump(pip_template, f, indent=2)
    
#     return pip_id


# def generate_pipeline_template(name=""):
#     # Define a basic pipeline template
#     pipeline_template = {
#         "name": name,
#         "nodes": [],
#         }
#     return pipeline_template



def check_node_exist(settings, node_id):
    pth_node_folder = settings["node_folder"]
    node_folder_path = f"{pth_node_folder}/n_{node_id}"
    
    # Check if the node folder exists
    return os.path.exists(node_folder_path)

# def generate_node_dic(node_id, dependencies=[]):
#     # Define a basic node template
#     node_dic = {
#         "nid": node_id,
#         "dependencies": dependencies,
#     }
#     return node_dic


def graph_to_dict(graph):
    pipeline_data = {
        'name': graph.name, # This is the tag that can be given to the pipeline
        'nodes': []
    }
    for node in graph.nodes:
        dependencies = list(graph.predecessors(node))
        pipeline_data['nodes'].append({
            'nid': node,
            'parents': dependencies
        })
    return pipeline_data


def graph_to_db(Gnx, cur):
    # Add the pipeline to the database
    pip_id = Gnx.graph['id']
    graph_tag = Gnx.graph.get('tag', None)
    pipeline_db.add_pipeline(cur, pipeline_id=pip_id, tag=graph_tag)

    # Add nodes and their dependencies directly from the graph
    for node in Gnx.nodes:
        pipeline_db.add_node_to_nodes(cur, node_id=node)
        pipeline_db.add_node_tag(cur, node_id=node, pipeline_id=pip_id, tag=Gnx.nodes[node].get("tag", None))
        for parent in Gnx.predecessors(node):
            pipeline_db.add_node_relation(cur, child_id=node, parent_id=parent)
        pipeline_db.add_node_to_pipeline(cur, node_id=node, pipeline_id=pip_id)
        pipeline_db.update_node_status(cur, node_id=node, status=Gnx.nodes[node].get("status", "null"))

def db_to_graph_from_pip_id(cur, pip_id):
    # Load the pipeline from the database
    if not pipeline_db.check_pipeline_exists(cur, pip_id):
        raise ValueError(f"Pipeline with ID {pip_id} does not exist in the database.")
 
    # Create a directed graph
    G = nx.DiGraph()
    G.graph['tag'] = pipeline_db.get_pipeline_tag(cur, pipeline_id=pip_id)  # Set the graph tag from the pipeline data
    G.graph['id'] = pip_id  # Set the graph ID from the pipeline data

    # Add nodes and their dependencies
    for node_id in pipeline_db.get_all_nodes_from_pip_id(cur, pipeline_id=pip_id):
        G.add_node(node_id)  # Add node with attributes
        # Get the status of the node from the database and add as an attribute
        status = pipeline_db.get_node_status(cur, node_id=node_id)
        if status not in NodeState._value2member_map_:
            raise ValueError(f"Invalid status '{status}' for node {node_id}. Must be one of {list(NodeState._value2member_map_.keys())}.")
        G.nodes[node_id]['status'] = status

        # Add edges based on parent relationships
        for parent_id in pipeline_db.get_node_parents(cur, node_id=node_id):
            G.add_edge(parent_id, node_id)
    
        # Add node tag if it exists
        tag = pipeline_db.get_node_tag(cur, node_id=node_id, pipeline_id=pip_id)
        if tag:
            G.nodes[node_id]['tag'] = tag

    return G





# def serialize_pipeline(settings, graph, pip_id = None, verbose=False):
#     pipeline_data = graph_to_dict(graph)
#     pipeline_id = pip_id if pip_id else generate_pip_id()
#     path_pip_file = f"{settings['pipeline_folder']}/{pipeline_id}.json"
#     with open(path_pip_file, 'w') as file:
#         json.dump(pipeline_data, file, indent=2)
#     if verbose:
#         print(f"Pipeline {pipeline_id} saved successfully.")

# def duplicate_pip(settings, graph):
#     new_pip_id = create_pipeline_file(settings)
#     save_pipeline(settings, pip_id=new_pip_id, verbose=True)

def add_node(settings, graph, node_id, dependencies=None):
    if not check_node_exist(settings, node_id):
        print(f"Node {node_id} does not exists.")
        return
    if dependencies is None:
        dependencies = []
    graph.add_node(node_id)
    for dep in dependencies:
        graph.add_edge(dep, node_id)
    return graph

# def detach_node(graph, node_id):
#     if node_id in graph:
#         # Remove all edges connected to the node
#         graph.remove_edges_from(list(graph.in_edges(node_id)) + list(graph.out_edges(node_id)))
#         print(f"All edges of node {node_id} have been removed. The node is now independent.")
#     else:
#         print(f"Node {node_id} does not exist in the pipeline.")
#     return graph

# def remove_input_edges(graph, node_id, verbose=False):
#     if node_id in graph:
#         # Remove all incoming edges to the node
#         graph.remove_edges_from(list(graph.in_edges(node_id)))
#         print(f"All input edges of node {node_id} have been removed.")
#     else:
#         print(f"Node {node_id} does not exist in the pipeline.")
#     return graph



def visualize_pip(graph):
    plt.figure(figsize=(10, 6))
    pos = nx.spring_layout(graph)

    # Define color mapping based on node status
    color_map = {
        "null": "gray",
        "failed": "red",
        "completed": "green",
        "running": "blue",
        "stale": "yellow"
    }

    # Get node colors based on their status
    node_colors = [
        color_map.get(graph.nodes[node].get("status", "null"), "gray")
        for node in graph.nodes
    ]

    nx.draw(
        graph,
        pos,
        with_labels=True,
        node_color=node_colors,
        node_size=2000,
        font_size=10
    )
    plt.title(f"Pipeline: {graph.name}")
    plt.show()

# def connect_node_to_parent(settings, graph, node_id, parent_node_id, verbose=False):
#     if not check_node_exist(settings, node_id):
#         print(f"Node {node_id} does not exist.")
#         return
#     if not check_node_exist(settings, parent_node_id):
#         print(f"Parent node {parent_node_id} does not exist.")
#         return
#     graph.add_edge(node_id, parent_node_id)
#     if verbose:
#         print(f"Node {node_id} connected to parent node {parent_node_id}.")
#     return graph

# def load_pipeline_dict_from_file(settings, pip_id):
#     pipeline_id = pip_id
#     path_pip_file = f"{settings['pipeline_folder']}/{pipeline_id}.json"
#     if os.path.exists(path_pip_file):
#         with open(path_pip_file, 'r') as file:
#             return json.load(file)
#     else:
#         raise FileNotFoundError(f"Pipeline file {path_pip_file} does not exist.")

# def load_pipeline_from_file(settings, pip_id):
#     pipeline_data = load_pipeline_dict_from_file(settings=settings, pip_id=pip_id)
#     # Create a directed graph
#     G = nx.DiGraph()
#     G.name = pipeline_data['name']  # Set the graph name from the pipeline data
#     for node in pipeline_data['nodes']:
#         if not check_node_exist(settings, node['nid']):
#             raise ValueError(f"Node {node['nid']} does not exist in the node folder.")
#         G.add_node(node['nid'])  # Add node with attributes
#         if 'parents' in node:
#             for dep in node['parents']:
#                 G.add_edge(dep, node['nid'])  # Add edges based on dependencies
#     # Load the status of each node and add it as an attribute to the graph
#     for node in G.nodes:
#         node_status_file = f"{settings['node_folder']}/n_{node}/meta.json"
#         if os.path.exists(node_status_file):
#             with open(node_status_file, 'r') as file:
#                 node_meta = json.load(file)
#             status = node_meta.get('status', 'null')
#         else:
#             status = "null"  # Default status if .status file is missing
#         if status not in NodeState._value2member_map_:
#             raise ValueError(f"Invalid status '{status}' for node {node}. Must be one of {list(NodeState._value2member_map_.keys())}.")
#         G.nodes[node]['status'] = status
#     return G


def generate_data_folder(base_path):
    os.makedirs(os.path.join(base_path, "nodes"), exist_ok=True)
    
    return {
        "nodes": os.path.join(base_path, "nodes"),
    }


# def save_pipeline(verbose=False):
#     pipeline_data = self.graph_to_dict()
#     pipeline_id = self.pip_id if self.pip_id else generate_id()
#     path_pip_file = f"{self.settings['pipeline_folder']}/p_{pipeline_id}.json"
#     with open(path_pip_file, 'w') as file:
#         json.dump(pipeline_data, file, indent=2)
#     if verbose:
#         print(f"Pipeline {pipeline_id} saved successfully.")


# # In memory object for 1 pipeline
# class DAG:
#     def __init__(self, settings, pip_id=None):
#         self.settings = settings
#         self.graph = nx.DiGraph()  # Initialize an empty directed graph
#         self.pip_id = None  # Initialize pipeline ID
#         if pip_id is not None:
#             self.set_pip_id(pip_id)
#             self.load_pipeline_from_file()

#     def set_pip_id(self, pip_id):
#         self.pip_id = pip_id

#     def duplicate_pip(self,settings):
#         new_pip_id = create_pipeline_file(settings)
#         self.set_pip_id(new_pip_id)
#         self.save_pipeline()

#     def load_pipeline_dict(self):
#         pipeline_id = self.pip_id
#         path_pip_file = f"{self.settings['pipeline_folder']}/p_{pipeline_id}.json"
#         if os.path.exists(path_pip_file):
#             with open(path_pip_file, 'r') as file:
#                 return json.load(file)
#         else:
#             raise FileNotFoundError(f"Pipeline file {path_pip_file} does not exist.")
        
#     def load_pipeline_from_file(self):
#         pipeline_data = self.load_pipeline_dict()
#         # Create a directed graph
#         G = nx.DiGraph()
#         G.name = pipeline_data['name']  # Set the graph name from the pipeline data
#         for node in pipeline_data['nodes']:
#             if not check_node_exist(self.settings, node['nid']):
#                 raise ValueError(f"Node {node['nid']} does not exist in the node folder.")
#             G.add_node(node['nid'])  # Add node with attributes
#             if 'dependencies' in node:
#                 for dep in node['dependencies']:
#                     G.add_edge(dep, node['nid'])  # Add edges based on dependencies
#         # Load the status of each node and add it as an attribute to the graph
#         for node in G.nodes:
#             node_status_file = f"{self.settings['node_folder']}/n_{node}/meta.json"
#             if os.path.exists(node_status_file):
#                 with open(node_status_file, 'r') as file:
#                     node_meta = json.load(file)
#                 status = node_meta.get('status', 'null')
#             else:
#                 status = "null"  # Default status if .status file is missing
#             if status not in NodeState._value2member_map_:
#                 raise ValueError(f"Invalid status '{status}' for node {node}. Must be one of {list(NodeState._value2member_map_.keys())}.")
#             G.nodes[node]['status'] = status
        
#         self.graph = G
