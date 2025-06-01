from datetime import datetime
import random
import os
import networkx as nx
import matplotlib.pyplot as plt
import json
from fusionpipe.utils import db_utils


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

def init_node_folder(node_folder_path, node_id, verbose=False):
    
    # Create the node folder path
    node_folder_path = f"{node_folder_path}/{node_id}"
    
    # Create the main node folder
    os.makedirs(node_folder_path, exist_ok=True)
    
    # Create subfolders 'code' and 'data'
    os.makedirs(f"{node_folder_path}/code", exist_ok=True)
    os.makedirs(f"{node_folder_path}/data", exist_ok=True)
    os.makedirs(f"{node_folder_path}/reports", exist_ok=True)
    
    if verbose:
        print(f"Node folder created at: {node_folder_path}")

def delete_node_folder(node_folder_path, node_id, verbose=False):
    
    # Construct the node folder path
    node_folder_path = f"{node_folder_path}/n_{node_id}"
    
    # Remove the node folder and its contents
    if os.path.exists(node_folder_path):
        os.rmdir(node_folder_path)
        if verbose:
            print(f"Node folder deleted: {node_folder_path}")
    else:
        if verbose:
            print(f"Node folder does not exist: {node_folder_path}")


def graph_to_dict(graph):
    """
    Convert a NetworkX directed graph to a dictionary format suitable for serialization.
    """

    pipeline_data = {
        'pipeline_id': graph.graph.get('pipeline_id'), # This is the tag that can be given to the pipeline
        'nodes': {},
        'notes': graph.graph.get('notes', None),  # Optional notes for the pipeline
        'tag': graph.graph.get('tag', None),  # Optional tag for the pipeline
        'owner': graph.graph.get('owner', None),  # Optional owner for the pipeline
    }
    for node in graph.nodes:
        parents = list(graph.predecessors(node))
        pipeline_data['nodes'][node] = {
            'parents': parents,
            'status': graph.nodes[node].get('status', 'null'),  # Default status is 'null'
            'editable': graph.nodes[node].get('editable', True),  # Default editable is True
            'tag': graph.nodes[node].get('tag', None),  # Optional tag for the node
            'notes': graph.nodes[node].get('notes', None),  # Optional notes for the node
        }
    return pipeline_data

def graph_dict_to_json(graph_dict, file_path, verbose=False):
    """
    Serialize a graph dictionary to a JSON file.
    """
    with open(file_path, 'w') as f:
        json.dump(graph_dict, f, indent=2)

    if verbose:
        print(f"Graph data saved to {file_path}")

def graph_dict_to_db(graph_dict, cur):
    # Insert pipeline using graph_dict fields
    pipeline_id = graph_dict["pipeline_id"]
    tag = graph_dict["tag"]
    owner = graph_dict["owner"]
    notes = graph_dict["notes"]

    if not db_utils.check_pipeline_exists(cur, pipeline_id):
        # If the pipeline does not exist, create it
        db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag=tag, owner=owner, notes=notes)


        # Insert nodes using graph_dict fields
        for node_id, node_data in graph_dict["nodes"].items():
            status = node_data["status"]
            editable = int(node_data["editable"])
            node_notes = node_data["notes"]
            node_tag = node_data["tag"]

            db_utils.add_node_to_nodes(cur, node_id=node_id, status=status, editable=editable, notes=node_notes)
            db_utils.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=node_tag)
            db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)


        # Insert node relations (edges) using parents field
        for child_id, node_data in graph_dict["nodes"].items():
            for parent_id in node_data.get("parents", []):
                db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
        return cur

def graph_to_db(Gnx, cur):
    # Add the pipeline to the database
    pip_id = Gnx.graph['pipeline_id']
    graph_tag = Gnx.graph.get('tag', None)
    owner = Gnx.graph.get('owner', None)
    notes = Gnx.graph.get('notes', None)
    db_utils.add_pipeline(cur, pipeline_id=pip_id, tag=graph_tag, owner=owner, notes=notes)

    # Add nodes and their dependencies directly from the graph
    for node in Gnx.nodes:
        node_id = node
        status = Gnx.nodes[node].get('status', 'ready')  # Default status is 'ready'
        editable = Gnx.nodes[node].get('editable', True)  # Default editable is True
        node_notes = Gnx.nodes[node].get('notes', "")  # Optional notes for the node
        node_tag = Gnx.nodes[node].get('tag', None)  # Optional tag for the node

        # Add the node to the database
        db_utils.add_node_to_nodes(cur, node_id=node_id, status=status, editable=editable, notes=node_notes)
        db_utils.add_node_tag(cur, node_id=node_id, pipeline_id=pip_id, tag=node_tag)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pip_id)

        # Add parent-child relationships
        for parent in Gnx.predecessors(node):
            db_utils.add_node_relation(cur, child_id=node_id, parent_id=parent)

def db_to_graph_from_pip_id(cur, pip_id):
    # Load the pipeline from the database
    if not db_utils.check_pipeline_exists(cur, pip_id):
        raise ValueError(f"Pipeline with ID {pip_id} does not exist in the database.")
 
    # Create a directed graph
    G = nx.DiGraph()
    G.graph['pipeline_id'] = pip_id  # Set the graph ID from the pipeline data
    G.graph['notes'] = db_utils.get_pipeline_notes(cur, pipeline_id=pip_id)  # Optional notes for the pipeline
    G.graph['tag'] = db_utils.get_pipeline_tag(cur, pipeline_id=pip_id)  # Set the graph tag from the pipeline data
    G.graph['owner'] = db_utils.get_pipeline_owner(cur, pipeline_id=pip_id)  # Optional owner for the pipeline

    # Add nodes and their dependencies
    for node_id in db_utils.get_all_nodes_from_pip_id(cur, pipeline_id=pip_id):
        G.add_node(node_id)  # Add node with attributes
        # Get the status of the node from the database and add as an attribute
        status = db_utils.get_node_status(cur, node_id=node_id)
        if status not in NodeState._value2member_map_:
            raise ValueError(f"Invalid status '{status}' for node {node_id}. Must be one of {list(NodeState._value2member_map_.keys())}.")
        editable = db_utils.is_node_editable(cur, node_id=node_id)
        G.nodes[node_id]['status'] = status
        G.nodes[node_id]['editable'] = editable
        G.nodes[node_id]['notes'] = db_utils.get_node_notes(cur, node_id=node_id)

        # Add edges based on parent relationships
        for parent_id in db_utils.get_node_parents(cur, node_id=node_id):
            G.add_edge(parent_id, node_id)
    
        # Add node tag if it exists
        tag = db_utils.get_node_tag(cur, node_id=node_id, pipeline_id=pip_id)
        if tag:
            G.nodes[node_id]['tag'] = tag
        else:
            G.nodes[node_id]['tag'] = ""

    return G

def db_to_graph_dict_from_pip_id(cur, pip_id):
    graph = db_to_graph_from_pip_id(cur, pip_id)
    return graph_to_dict(graph)


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




# def generate_node_dic(node_id, dependencies=[]):
#     # Define a basic node template
#     node_dic = {
#         "nid": node_id,
#         "dependencies": dependencies,
#     }
#     return node_dic
