from datetime import datetime
import random
import os
import networkx as nx
import matplotlib.pyplot as plt
import json
from fusionpipe.utils import db_utils
import copy
from enum import Enum
import shutil
import toml
import stat


FILE_CHMOD_DEFAULT = 0o664  # Read and write for owner and group, read for others
DIR_CHMOD_DEFAULT = 0o775  # Read, write, and execute for owner and group, read and execute for others
FILE_CHMOD_BLOCKED = 0o444  # Read-only for owner, group, and others
DIR_CHMOD_BLOCKED = 0o555  # Read and execute for owner, group, and others, no write permission

def change_permissions_recursive(path, file_mode=FILE_CHMOD_DEFAULT, dir_mode=DIR_CHMOD_DEFAULT):
    """Recursively change permissions of a directory and its contents."""
    
    if not path:
        # Skip if not path provided
        return    
    if not os.path.exists(path):
        return

    # Avoid links, or it will change permission for python executable too.
    for root, dirs, files in os.walk(path):
        for d in dirs:
            dir_path = os.path.join(root, d)
            if not os.path.islink(dir_path):
                os.chmod(dir_path, dir_mode)
        for f in files:
            file_path = os.path.join(root, f)
            if os.path.isfile(file_path) and not os.path.islink(file_path):
                os.chmod(file_path, file_mode)

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

def generate_project_id():
    return "pr_" + generate_id()

def init_node_folder(folder_path_nodes, verbose=False):
    
    # Create the main node folder
    os.makedirs(folder_path_nodes, exist_ok=True)

    code_folder_path = os.path.join(folder_path_nodes,"code")
    node_examples_folder_path = os.path.join(code_folder_path, "examples")
    
    # Create subfolders 'code' and 'data'
    os.makedirs(f"{code_folder_path}", exist_ok=True)
    os.makedirs(node_examples_folder_path, exist_ok=True)
    os.makedirs(f"{folder_path_nodes}/data", exist_ok=True)
    os.makedirs(f"{folder_path_nodes}/reports", exist_ok=True)

    # Add __init__.py files to the examples folder to make it a package
    init_file_path = os.path.join(node_examples_folder_path, '__init__.py')
    if not os.path.exists(init_file_path):
        with open(init_file_path, 'w') as init_file:
            init_file.write("# This file makes the examples folder a package\n")

    # Save the current working directory
    current_dir = os.getcwd()

    try:
        # Run the 'uv init' command inside the node folder
        os.chdir(code_folder_path)
        # Initialize the uv environment with a specific name (if supported)
        env_name = os.path.basename(folder_path_nodes)
        os.system(f"uv init --name {env_name}")

        user_utils_folder = os.path.join(os.environ.get("USER_UTILS_FOLDER_PATH"))
        os.system(f"uv add --editable {user_utils_folder}")

        # Update the pyproject.toml file to replace the relative path with the absolute path.
        # This will allow ray to work with the external package.
        pyproject_file_path = os.path.join(code_folder_path, 'pyproject.toml')
        if os.path.exists(pyproject_file_path):
            with open(pyproject_file_path, 'r') as file:
                pyproject_data = toml.load(file)

            pyproject_data["tool"]["uv"]["sources"]["fp-user-utils"]["path"] = user_utils_folder

            # Write the updated data back to the file
            with open(pyproject_file_path, 'w') as file:
                toml.dump(pyproject_data, file)

        # Install the current environment as a Jupyter kernel
        os.system("uv run python -m ipykernel install --user --name " + env_name + " --display-name " + env_name)

        # Copy the template file into the code folder
        template_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates', 'main_template.py')
        if os.path.exists(template_file_path):
            destination_file_path = os.path.join(code_folder_path, 'main.py')        
            with open(template_file_path, 'r') as template_file:
                template_content = template_file.read()
            
            # Import and append commented examples from example_generator
            from fusionpipe.utils.example_generator import generate_commented_examples_for_main
            
            # Generate commented examples using the dedicated function
            example_section = generate_commented_examples_for_main()
            template_content += f"\n{example_section}\n"
            
            with open(destination_file_path, 'w') as dest_file:
                dest_file.write(template_content)
        else:
            raise FileNotFoundError(f"Template file not found at {template_file_path}")
        

        # Copy example files into the code folder
        template_folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
        example_python_path = os.path.join(template_folder_path, 'example_python.py')
        example_matlab_path = os.path.join(template_folder_path, 'example_matlab.m')
        example_notebook_path = os.path.join(template_folder_path, 'example_notebook.ipynb')
        init_node_kernel_path = os.path.join(template_folder_path, 'init_node_kernel.py')
        example_defuse_path = os.path.join(template_folder_path, 'DEFUSE_example.m')
        parmameter_yaml_path = os.path.join(template_folder_path, 'node_parameters.yaml')

        # Copy and grand writing access to the example files
        if os.path.exists(example_defuse_path):
            shutil.copy(example_defuse_path, node_examples_folder_path)
            os.chmod(os.path.join(node_examples_folder_path, "DEFUSE_example.m"), FILE_CHMOD_DEFAULT)

        if os.path.exists(parmameter_yaml_path):
            shutil.copy(parmameter_yaml_path, code_folder_path)
            os.chmod(os.path.join(code_folder_path, "node_parameters.yaml"), FILE_CHMOD_DEFAULT)
        else:
            raise FileNotFoundError(f"Parameters YAML file not found at {parmameter_yaml_path}")

        if os.path.exists(example_python_path):
            shutil.copy(example_python_path, node_examples_folder_path)
            os.chmod(os.path.join(node_examples_folder_path, "example_python.py"), FILE_CHMOD_DEFAULT)
        else:
            raise FileNotFoundError(f"Example Python file not found at {example_python_path}")
        
        if os.path.exists(init_node_kernel_path):
            shutil.copy(init_node_kernel_path, code_folder_path)
            os.chmod(os.path.join(code_folder_path, "init_node_kernel.py"), FILE_CHMOD_DEFAULT)
        else:
            raise FileNotFoundError(f"Example Python file not found at {init_node_kernel_path}")

        if os.path.exists(example_matlab_path):
            shutil.copy(example_matlab_path, node_examples_folder_path)
            os.chmod(os.path.join(node_examples_folder_path, "example_matlab.m"), FILE_CHMOD_DEFAULT)
        else:
            raise FileNotFoundError(f"Example MATLAB file not found at {example_matlab_path}")
        
        if os.path.exists(example_notebook_path):
            shutil.copy(example_notebook_path, node_examples_folder_path)
            os.chmod(os.path.join(node_examples_folder_path, "example_notebook.ipynb"), FILE_CHMOD_DEFAULT)
        else:
            raise FileNotFoundError(f"Example Notebook file not found at {example_notebook_path}")

        # Update the name entry in pyproject.toml using the toml library
        pyproject_file_path = os.path.join(code_folder_path, 'pyproject.toml')
        if os.path.exists(pyproject_file_path):
            with open(pyproject_file_path, 'r') as file:
                pyproject_data = toml.load(file)
                
            # Update the 'name' field
            pyproject_data['project']['name'] = os.path.basename(folder_path_nodes)
            
            # Write the updated data back to the file
            with open(pyproject_file_path, 'w') as file:
                toml.dump(pyproject_data, file)

        # Write the node ID to a .node_id file inside the code folder
        node_id_file_path = os.path.join(code_folder_path, '.node_id')
        with open(node_id_file_path, 'w') as node_id_file:
            node_id_file.write(os.path.basename(folder_path_nodes) + '\n')


    finally:
        # Change back to the previous working directory
        os.chdir(current_dir)


    # Commit all changes in the code folder path
    os.chdir(code_folder_path)
    os.system("git init")  # Initialize a git repository if not already initialized
    os.system("git add .")  # Add all changes to the staging area
    os.system('git commit -m "Initial commit for code folder"')  # Commit the changes
    

    # Run empty main to set-up the .venv
    os.system("uv run")
    os.chdir(current_dir)  # Change back to the original directory    
    
    if verbose:
        print(f"Node folder created at: {folder_path_nodes}")


def delete_node_folder(node_folder_path, verbose=False):
    
    # Remove the node folder and its contents
    if os.path.exists(node_folder_path):
        shutil.rmtree(node_folder_path)
        if verbose:
            print(f"Node folder deleted: {node_folder_path}")
    else:
        if verbose:
            print(f"Node folder does not exist: {node_folder_path}")


def pipeline_graph_to_dict(graph):
    """
    Convert a NetworkX directed graph to a dictionary format suitable for serialization.
    """
    

    pipeline_data = {
        'pipeline_id': graph.graph.get('pipeline_id'), # This is the tag that can be given to the pipeline
        'nodes': {},
        'notes': graph.graph.get('notes', None),  # Optional notes for the pipeline
        'tag': graph.graph.get('tag', None),  # Optional tag for the pipeline
        'owner': graph.graph.get('owner', None),  # Optional owner for the pipeline
        'project_id': graph.graph.get('project_id', ""),  # Optional list of project IDs associated with the pipeline
        'blocked': graph.graph.get('blocked', False),  # Optional blocked status for the pipeline
    }
    for node in graph.nodes:
        parents = list(graph.predecessors(node))
        pipeline_data['nodes'][node] = {
            'parents': parents,
            'parent_edge_ids': {parent: graph.edges[parent, node].get('edge_id') for parent in parents},  # Edge IDs for each parent
            'status': graph.nodes[node].get('status', 'null'),  # Default status is 'null'
            'referenced': graph.nodes[node].get('referenced', False),  # Default referenced is True
            'tag': graph.nodes[node].get('tag', None),  # Optional tag for the node
            'notes': graph.nodes[node].get('notes', None),  # Optional notes for the node
            'position': graph.nodes[node].get('position', None),  # Node position
            'folder_path': graph.nodes[node].get('folder_path', None),  # Optional folder path for the node
            'blocked': graph.nodes[node]['blocked']
        }
    return pipeline_data


def project_graph_to_dict(graph):
    """
    Convert a NetworkX directed graph to a dictionary format suitable for serialization.
    """

    project_data = {
        'project_id': graph.graph.get('project_id'),  # This is the tag that can be given to the project
        'nodes': {},
        'notes': graph.graph.get('notes', None),  # Optional notes for the project
        'tag': graph.graph.get('tag', None),  # Optional tag for the project
        'owner': graph.graph.get('owner', None),  # Optional owner for the project
    }
    for node in graph.nodes:
        parents = list(graph.predecessors(node))
        project_data['nodes'][node] = {
            'parents': parents,
            'tag': graph.nodes[node].get('tag', None),  # Optional tag for the node
            'notes': graph.nodes[node].get('notes', None),  # Optional notes for the node
            'blocked': graph.nodes[node].get('blocked', False),  # Default referenced is True
        }
    return project_data


def graph_dict_to_json(graph_dict, file_path, verbose=False):
    """
    Serialize a graph dictionary to a JSON file.
    """
    with open(file_path, 'w') as f:
        json.dump(graph_dict, f, indent=2)

    if verbose:
        print(f"Graph data saved to {file_path}")


def pipeline_graph_to_db(Gnx, cur):
    """
    - This function can be used to add a pipeline graph to the pipelines table
    - This will also work to add a subgraph to an existing pipeline.

    Special consideration:
    - Nodes which are referenced, hence that are present in multiple pipelines, must have the same parents in all pipelines,
    but can have different childrens in different pipelines. This guarantees that a subgraph of referenced nodes preserve the same inputs across pipelines.
    To enforce that, the database has to be traversing the graph, and add the parents of each nodes.
    """
    # Add the pipeline to the database
    pip_id = Gnx.graph['pipeline_id']
    graph_tag = Gnx.graph.get('tag', None)
    owner = Gnx.graph.get('owner', None)
    notes = Gnx.graph.get('notes', None)
    project_id = Gnx.graph.get('project_id', generate_project_id())
    blocked = Gnx.graph.get('blocked', False)

    # Check if the project associated with the pipeline exists, if not, add it
    if project_id and not db_utils.check_if_project_exists(cur, project_id):
        db_utils.add_project(cur, project_id=project_id)
    
    # Check if the pipeline already exists, if not, add it
    if not db_utils.check_if_pipeline_exists(cur, pip_id):
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pip_id, tag=graph_tag, owner=owner, notes=notes, project_id=project_id, blocked=blocked)

    # Add nodes and their dependencies directly from the graph
    for node in Gnx.nodes:
        node_id = node
        status = Gnx.nodes[node].get('status', 'ready')  # Default status is 'ready'
        referenced = Gnx.nodes[node].get('referenced', False)  # Default referenced is False
        node_notes = Gnx.nodes[node].get('notes', "")  # Optional notes for the node
        node_tag = Gnx.nodes[node].get('tag', None)  # Optional tag for the node
        position = Gnx.nodes[node].get('position', None)  # Node position
        folder_path = Gnx.nodes[node].get('folder_path', None)  # Optional folder path for the node
        blocked = Gnx.nodes[node].get('blocked')  # Optional blocked status for the node

        # Add the node to the database
        if not db_utils.check_if_node_exists(cur, node):
            db_utils.add_node_to_nodes(
                cur,
                node_id=node_id,
                status=status,
                referenced=referenced,
                notes=node_notes,
                folder_path=folder_path,
                node_tag=node_tag,
                blocked=blocked
            )
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pip_id, 
                                position_x=position[0], position_y=position[1])
    # Add relation between nodes
    for node in Gnx.nodes:
        for parent in Gnx.predecessors(node):
            if not db_utils.check_node_relation_exists(cur, child_id=node, parent_id=parent):
                connect_node(cur, child_id=node, parent_id=parent)


def project_graph_to_db(Gnx, cur):
    """
    - This function can be used to add a graph to the database.
    - This will also work to add a subgraph to an existing pipeline.
    """
    # Add the project to the database
    project_id = Gnx.graph['project_id']
    graph_tag = Gnx.graph.get('tag', None)
    owner = Gnx.graph.get('owner', None)
    notes = Gnx.graph.get('notes', None)

    # Check if the project already exists, if not, add it
    if not db_utils.check_project_exists(cur, project_id):
        db_utils.add_project(cur, project_id=project_id, tag=graph_tag, owner=owner, notes=notes)

    # Add pipeline and their dependencies directly from the graph
    for pipeline_id in Gnx.nodes:
        pipeline_notes = Gnx.nodes[pipeline_id].get('notes', "")  # Optional notes for the pipeline
        pipeline_tag = Gnx.nodes[pipeline_id].get('tag', None)  # Optional tag for the pipeline

        # Add the pipeline to the database
        if not db_utils.check_if_pipeline_exists(cur, pipeline_id):
            db_utils.add_pipeline_to_pipelines(
                cur,
                pipeline_id=pipeline_id,
                tag=pipeline_tag,
                notes=pipeline_notes,
                project_id=project_id
                )
            # If node already existed, parents cannot have changed. Children can and will be added later
            for parent in Gnx.predecessors(pipeline_id):
                db_utils.add_pipeline_relation(cur, child_id=pipeline_id, parent_id=parent)


def db_to_pipeline_graph_from_pip_id(cur, pip_id):
    '''
    Special consideration:
    - Nodes which are referenced, hence that are present in multiple pipelines, must have the same parents in all pipelines,
    but can have different childrens in different pipelines. This guarantees that a subgraph of referenced nodes preserve the same inputs across pipelines.
    To enforce that, the database has to be traversing the graph, and add the parents of each nodes.
    '''
    # Load the pipeline from the database
    if not db_utils.check_if_pipeline_exists(cur, pip_id):
        raise ValueError(f"Pipeline with ID {pip_id} does not exist in the database.")
 
    # Create a directed graph
    G = nx.DiGraph()
    G.graph['pipeline_id'] = pip_id  # Set the graph ID from the pipeline data
    G.graph['notes'] = db_utils.get_pipeline_notes(cur, pipeline_id=pip_id)  # Optional notes for the pipeline
    G.graph['tag'] = db_utils.get_pipeline_tag(cur, pipeline_id=pip_id)  # Set the graph tag from the pipeline data
    G.graph['owner'] = db_utils.get_pipeline_owner(cur, pipeline_id=pip_id)  # Optional owner for the pipeline
    G.graph['project_id'] = db_utils.get_project_id_by_pipeline(cur, pipeline_id=pip_id)  # Optional list of project IDs associated with the pipeline
    G.graph['blocked'] = db_utils.get_pipeline_blocked_status(cur, pipeline_id=pip_id)  # Optional blocked status for the pipeline

    # Add nodes and their dependencies
    for node_id in db_utils.get_all_nodes_from_pip_id(cur, pipeline_id=pip_id):
        G.add_node(node_id)  # Add node with attributes
        # Get the status of the node from the database and add as an attribute
        status = db_utils.get_node_status(cur, node_id=node_id)
        if status not in NodeState._value2member_map_:
            raise ValueError(f"Invalid status '{status}' for node {node_id}. Must be one of {list(NodeState._value2member_map_.keys())}.")
        referenced = db_utils.get_node_referenced_status(cur, node_id=node_id)
        G.nodes[node_id]['status'] = status
        G.nodes[node_id]['referenced'] = referenced
        G.nodes[node_id]['notes'] = db_utils.get_node_notes(cur, node_id=node_id)
        G.nodes[node_id]['folder_path'] = db_utils.get_node_folder_path(cur, node_id=node_id)
        G.nodes[node_id]['blocked'] = db_utils.get_node_blocked_status(cur, node_id=node_id)

        # Add position if available
        position = db_utils.get_node_position(cur, node_id=node_id, pipeline_id=pip_id)
        if position:
            G.nodes[node_id]['position'] = position

        # Add edges based on parent relationships
        for parent_id in db_utils.get_node_parents(cur, node_id=node_id):
            edge_id = db_utils.get_node_relation_edge_id(cur, child_id=node_id, parent_id=parent_id)
            G.add_edge(parent_id, node_id, edge_id=edge_id)  # Add edge with edge_id as an attribute
    
        # Add node tag if it exists
        tag = db_utils.get_node_tag(cur, node_id=node_id)
        if tag:
            G.nodes[node_id]['tag'] = tag
        else:
            G.nodes[node_id]['tag'] = ""

    return G


def db_to_project_graph_from_project_id(cur, project_id):
    # Load the pipeline from the database
    if not db_utils.check_if_project_exists(cur, project_id):
        raise ValueError(f"Project with ID {project_id} does not exist in the database.")

    # Create a directed graph
    G = nx.DiGraph()
    G.graph['project_id'] = project_id  # Set the graph ID from the pipeline data
    G.graph['notes'] = db_utils.get_project_notes(cur, project_id=project_id)  # Optional notes for the project
    G.graph['tag'] = db_utils.get_project_tag(cur, project_id=project_id)  # Set the graph tag from the project data
    G.graph['owner'] = db_utils.get_project_owner(cur, project_id=project_id)  # Optional owner for the project

    # Add nodes and their dependencies
    for pipeline_id in db_utils.get_all_pipelines_from_project_id(cur, project_id=project_id):
        G.add_node(pipeline_id)  # Add node with attributes
        G.nodes[pipeline_id]['notes'] = db_utils.get_pipeline_notes(cur, pipeline_id=pipeline_id)
        G.nodes[pipeline_id]['blocked'] = db_utils.get_pipeline_blocked_status(cur, pipeline_id=pipeline_id)

        # Add edges based on parent relationships
        for parent_id in db_utils.get_pipeline_parents(cur, pipeline_id=pipeline_id):
            G.add_edge(parent_id, pipeline_id)

        # Add node tag if it exists
        tag = db_utils.get_pipeline_tag(cur, pipeline_id=pipeline_id)
        if tag:
            G.nodes[pipeline_id]['tag'] = tag
        else:
            G.nodes[pipeline_id]['tag'] = ""

    return G


def db_to_pipeline_dict_from_pip_id(cur, pip_id):
    graph = db_to_pipeline_graph_from_pip_id(cur, pip_id)
    return pipeline_graph_to_dict(graph)

def db_to_project_dict_from_project_id(cur, project_id):
    graph = db_to_project_graph_from_project_id(cur, project_id)
    return project_graph_to_dict(graph)


def dict_to_graph(graph_dict):
    """
    Convert a dictionary representation of a graph back to a NetworkX directed graph.
    """
    G = nx.DiGraph()
    
    # Set graph attributes
    G.graph['pipeline_id'] = graph_dict.get('pipeline_id', None)
    G.graph['notes'] = graph_dict.get('notes', None)
    G.graph['tag'] = graph_dict.get('tag', None)
    G.graph['owner'] = graph_dict.get('owner', None)
    G.graph['blocked'] = graph_dict.get('blocked', False)

    # Add nodes and their attributes
    for node_id, node_data in graph_dict['nodes'].items():
        G.add_node(node_id, status=node_data.get('status', 'ready'), referenced=node_data['referenced'],
                   tag=node_data.get('tag', None), notes=node_data.get('notes', None),
                   folder_path=node_data.get('folder_path', None),
                   blocked=node_data.get('blocked', False))
        for parent in node_data.get('parents', []):
            G.add_edge(parent, node_id)

    return G

def visualize_pip_static(graph):
    plt.figure(figsize=(10, 6))
    pos = nx.spring_layout(graph)

    # Define color mapping based on node status
    color_map = {
        "ready": "gray",
        "failed": "red",
        "completed": "green",
        "running": "blue",
        "staledata": "yellow"
    }

    # Get node colors based on their status
    node_colors = [
        color_map.get(graph.nodes[node].get("status", "ready"), "gray")
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

def generate_data_folder(base_path):
    os.makedirs(os.path.join(base_path, "nodes"), exist_ok=True)
    
    return {
        "nodes": os.path.join(base_path, "nodes"),
    }

def visualize_pip_interactive(graph, output_file="pipeline_visualization.html"):
    from pyvis.network import Network 
    """
    Visualize the pipeline graph interactively using pyvis.
    """
    # Create a new Network object
    net = Network(height="750px", width="100%", directed=True)
    
    # Define color mapping based on node status
    color_map = {
        "ready": "gray",
        "failed": "red",
        "completed": "green",
        "running": "blue",
        "staledata": "yellow"
    }

    # Add nodes with attributes
    for node in graph.nodes:
        status = graph.nodes[node].get("status", "ready")
        color = color_map.get(status, "gray")
        title = " ".join([f"{key}: {value}" for key, value in graph.nodes[node].items()])
        net.add_node(node, label=node, color=color, title=title)

    # Add edges
    for edge in graph.edges:
        net.add_edge(edge[0], edge[1])

    # Generate and save the interactive visualization
    net.write_html(output_file)

def branch_pipeline_from_node(cur, pipeline_id, node_id):
    """
    Branch a pipeline from a node means:
    - Copy the pipeline.
    - All nodes are preserved, except the specified node and all its descendants,
      for which new nodes are created (with new IDs).
    """

    # Get the original graph from the database
    original_graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)
    
    # Generate a new pipeline ID and tag
    new_pip_id = generate_pip_id()

    # Find all descendants of the specified node (children, grandchildren, etc.)
    descendants = nx.descendants(original_graph, node_id)

    # Include the node itself
    nodes_to_replace = set(descendants) | {node_id}

    # Map old node IDs to new node IDs for replaced nodes
    id_map = {old_id: generate_node_id() for old_id in nodes_to_replace}

    # Create a new graph for the new pipeline
    new_graph = nx.DiGraph()
    new_graph.graph.update(original_graph.graph)
    new_graph.graph['pipeline_id'] = new_pip_id
    new_graph.graph['tag'] = new_pip_id

    # Add nodes: copy all nodes, but for nodes_to_replace use new IDs
    for n in original_graph.nodes:
        if n in nodes_to_replace:
            new_id = id_map[n]
            attrs = original_graph.nodes[n].copy()
            new_graph.add_node(new_id, **attrs)
        else:
            attrs = original_graph.nodes[n].copy()
            new_graph.add_node(n, **attrs)

    # Add edges: remap edges for replaced nodes
    for u, v in original_graph.edges:
        u_new = id_map[u] if u in nodes_to_replace else u
        v_new = id_map[v] if v in nodes_to_replace else v
        new_graph.add_edge(u_new, v_new)

    # Add the new graph to the database
    pipeline_graph_to_db(new_graph, cur)

    # Add pipeline-to-project relation for the new pipeline
    db_utils.add_pipeline_relation(cur, child_id=new_pip_id, parent_id=pipeline_id)

    return new_pip_id

def delete_node_from_pipeline_with_referenced_logic(cur,pipeline_id, node_id):
    if db_utils.get_pipeline_blocked_status(cur, pipeline_id=pipeline_id):
        raise ValueError(f"Pipeline {pipeline_id} is blocked and cannot delete node {node_id}.")

    if db_utils.get_node_blocked_status(cur, node_id=node_id):
        raise ValueError(f"Node {node_id} is blocked and cannot be deleted.")

    # Deal with reference logics
    if not db_utils.get_node_referenced_status(cur, node_id=node_id):
        # If not referenced, delete the node directly from the pipeline database
        set_children_stale(cur, pipeline_id, node_id)
        db_utils.remove_node_from_pipeline(cur, pipeline_id=pipeline_id, node_id=node_id)
        delete_node_folder(db_utils.get_node_folder_path(cur, node_id=node_id))
    else:
        # Check if the node is a leaf (no children) of a referenced subgraph or is has no parents
        if not node_is_leaf_of_referenced_subgraph(cur, pipeline_id, node_id):
            raise ValueError(f"Node {node_id} is not a leaf of referenced subgraph.")

        # Delete the node from the pipeline
        db_utils.remove_node_from_pipeline(cur, pipeline_id=pipeline_id, node_id=node_id)

    # Update R/W permission for nodes
    update_referenced_node_permissions(cur, node_id)

def node_is_leaf_of_referenced_subgraph(cur, pipeline_id, node_id):
    # Get the pipeline graph from the database
    graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)

    # Get subgraph of referenced nodes
    referenced_nodes = [n for n in graph.nodes if graph.nodes[n]['referenced']]
    subgraph = graph.subgraph(referenced_nodes).copy()

    return not list(subgraph.successors(node_id))

def node_is_leaf_of_subgraph(cur, pipeline_id, node_id):
    """
    Check if a node is a leaf of the subgraph of referenced 
    A leaf node is a node that has no children in the subgraph.
    """
    # Get the pipeline graph from the database
    graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)

    return not list(graph.successors(node_id))

def node_is_head_of_subgraph(cur, pipeline_id, node_id):
    """
    Check if a node is a head of the subgraph of referenced 
    A head node is a node that has no parents in the subgraph.
    """
    # Get the pipeline graph from the database
    graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)

    return not list(graph.predecessors(node_id))

def can_node_be_referenced(cur, pipeline_id, node_id):
    # Check if the node is a head of the subgraph
    return node_is_head_of_subgraph(cur, pipeline_id, node_id)

def can_node_run(cur, node_id):
    # Check if the node is in 'ready' state
    status = db_utils.get_node_status(cur, node_id=node_id)
    if status != NodeState.READY.value:
        return False
    
    # Cannot run node if bloceked
    if db_utils.get_node_blocked_status(cur, node_id=node_id):
        return False
    
    # Cannot run node if referenced
    if db_utils.get_node_referenced_status(cur, node_id=node_id):
        return False    

    # Get all parent nodes
    parent_ids = db_utils.get_node_parents(cur, node_id=node_id)
    # Check if all parents are in 'completed' state
    for parent_id in parent_ids:
        parent_status = db_utils.get_node_status(cur, node_id=parent_id)
        if parent_status != NodeState.COMPLETED.value:
            return False
    # If node has no parents, it can run
    return True


def get_all_descendants(cur, pipeline_id, node_id):
    """
    Get all children of a node in the pipeline.
    """
    graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)
    return list(nx.descendants(graph, node_id))

def copy_with_permissions(src, dst, *, follow_symlinks=True):
    """Custom copy function to ensure destination files are writable."""
    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))
    try:
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
    except PermissionError:
        # If we get a permission error, it's likely because the destination
        # file is read-only. Change permissions and try again.
        os.chmod(dst, stat.S_IWUSR)
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
    # Ensure the destination file is writable
    os.chmod(dst, os.stat(dst).st_mode | stat.S_IWUSR)
    return dst


def duplicate_node_in_pipeline_w_code_and_data(cur, source_pipeline_id, target_pipeline_id, source_node_id, new_node_id, parents=False, childrens=False, withdata=False):
    """
    Duplicate a node in the pipeline, including its code and data.
    """
    # Duplicate node in the database
    db_utils.duplicate_node_in_pipeline_with_relations(
        cur,
        source_node_id, 
        new_node_id,
        source_pipeline_id,
        target_pipeline_id,
        parents=parents,
        childrens=childrens,
        )

    # New node is not referenced, even if original was
    db_utils.update_referenced_status(cur, node_id=new_node_id, referenced=False)

    # Copy the folder into the new node folder
    new_folder_path_nodes = os.path.join(os.environ.get("FUSIONPIPE_DATA_PATH"), new_node_id)
    # Update database
    db_utils.update_folder_path_node(cur, new_node_id, new_folder_path_nodes)
    source_node_tag = db_utils.get_node_tag(cur, node_id=source_node_id)
    db_utils.update_node_tag(cur, node_id=new_node_id, node_tag=source_node_tag)

    old_folder_path_nodes = db_utils.get_node_folder_path(cur, node_id=source_node_id)

    if old_folder_path_nodes:
        # Ensure the source .git directory is readable before copying
        source_git_dir = os.path.join(old_folder_path_nodes, 'code', '.git')
        if os.path.exists(source_git_dir):
            change_permissions_recursive(source_git_dir, dir_mode=DIR_CHMOD_DEFAULT)

        # Copy only the 'code' and 'reports' subfolders, skipping '.venv'
        os.makedirs(new_folder_path_nodes, exist_ok=True)

        # Initise the new node folder
        init_node_folder(new_folder_path_nodes, verbose=False)

        for subfolder in ['code', 'reports']:
            old_subfolder_path = os.path.join(old_folder_path_nodes, subfolder)
            new_subfolder_path = os.path.join(new_folder_path_nodes, subfolder)
            if os.path.exists(old_subfolder_path):
                # Use ignore to skip .venv if present in subfolder
                shutil.copytree(
                    old_subfolder_path,
                    new_subfolder_path,
                    dirs_exist_ok=True,
                    copy_function=copy_with_permissions,
                    ignore=shutil.ignore_patterns('.venv', '__pycache__', '*.pyc', '*.pyo', '*.pyd', '*.ipynb_checkpoints', '.node_id')
                )

        # Ensure the new .git directory is writable after copying
        new_git_dir = os.path.join(new_folder_path_nodes, 'code', '.git')
        if os.path.exists(new_git_dir):
            change_permissions_recursive(new_git_dir, dir_mode=DIR_CHMOD_DEFAULT)

        if withdata:
            old_data_folder = os.path.join(old_folder_path_nodes, "data")
            new_data_folder = os.path.join(new_folder_path_nodes, "data")
            if os.path.exists(old_data_folder):
                shutil.copytree(old_data_folder, new_data_folder, dirs_exist_ok=True)
            else:
                os.makedirs(new_data_folder, exist_ok=True)
        else:
            os.makedirs(os.path.join(new_folder_path_nodes, "data"), exist_ok=True)

    # Update read and write permission for the new node
    update_referenced_node_permissions(cur, node_id=new_node_id)
    

    # Load and update the project.toml file
    project_toml_path = os.path.join(new_folder_path_nodes, "code", "pyproject.toml")
    if os.path.exists(project_toml_path):
        with open(project_toml_path, 'r') as file:
            pyproject_data = toml.load(file)

        # Update the 'name' field with the new node ID
        pyproject_data['project']['name'] = new_node_id

        # Write the updated data back to the file
        with open(project_toml_path, 'w') as file:
            toml.dump(pyproject_data, file)

    # Initialize the .venv using uv
    code_folder_path = os.path.join(new_folder_path_nodes, "code")
    current_dir = os.getcwd()
    try:
        os.chdir(code_folder_path)
        # Set up the virtual environment for the new node.
        os.system("uv run")
        # Add the ipykernel package to the virtual environment of the core user, in order to be able to run the node in Jupyter
        os.system("uv run python -m ipykernel install --user --name " + new_node_id + " --display-name " + new_node_id)

    finally:
        os.chdir(current_dir)

def duplicate_nodes_in_pipeline_with_relations(cur, source_pipeline_id, target_pipeline_id, source_node_ids, withdata=False):
    """
    Duplicate a list of nodes including their relation inside a pipeline.
    - source_node_ids: list of node ids to duplicate
    - withdata: if True, also copy the data folder of the nodes
    - source_pipeline_id: the pipeline id from which to duplicate the nodes
    - target_pipeline_id: the pipeline id to which to duplicate the nodes

    This function duplicates the specified nodes and their relations in the target pipeline.
    It creates new node IDs for the duplicated nodes and updates their parent-child relationships accordingly.
    It also shifts the positions of the duplicated nodes to avoid overlap.
    If `withdata` is True, it copies the data associated with the nodes.

    id_map: a dictionary mapping old node ids to new node ids
    """
    if (source_pipeline_id == target_pipeline_id) and (db_utils.get_pipeline_blocked_status(cur, pipeline_id=source_pipeline_id)):
        raise ValueError(f"Pipeline {source_pipeline_id} is blocked. Duplicate the node into this pipeline.")

    if isinstance(source_node_ids, str):
        source_node_ids = [source_node_ids]
    # Get the pipeline graph
    graph = db_to_pipeline_graph_from_pip_id(cur, source_pipeline_id)
    # Get all nodes in the subtree(s)
    subtree_nodes = set()
    for root in source_node_ids:
        subtree_nodes.add(root)
    # Build subgraph
    subtree = graph.subgraph(subtree_nodes).copy()
    # Map old node ids to new node ids
    id_map = {old_id: generate_node_id() for old_id in subtree.nodes}
    # Duplicate nodes in topological order (parents before children)
    for old_id in nx.topological_sort(subtree):
        new_id = id_map[old_id]
        # Duplicate node in DB (without parents/children relations)
        duplicate_node_in_pipeline_w_code_and_data(
            cur, source_pipeline_id,target_pipeline_id, old_id, new_id, parents=False, childrens=False, withdata=withdata)
    # Set parent-child relations in the duplicated subtree
    for old_parent, old_child in subtree.edges:
        connect_node(
            cur,
            child_id=id_map[old_child],
            parent_id=id_map[old_parent]
        )

    # Find head nodes of the duplicated subtree (nodes with no incoming edges in the subtree)
    head_nodes = [n for n in subtree.nodes if subtree.in_degree(n) == 0]
    for old_head in head_nodes:
        new_head = id_map[old_head]
        # Find parents of the original head node in the full graph (outside the subtree)
        for parent in graph.predecessors(old_head):
            if parent not in subtree_nodes:
                # Attach the new head node to the parent in the target pipeline
                connect_node(
                    cur,
                    child_id=new_head,
                    parent_id=parent
                )
    
    # Optionally shift the positions of the duplicated nodes to avoid overlap
    shift_x, shift_y = 40, 40  # You can adjust the shift values as needed
    for old_id, new_id in id_map.items():
        position = db_utils.get_node_position(cur, node_id=old_id, pipeline_id=source_pipeline_id)
        if position is not None:
            new_position = (position[0] + shift_x, position[1] + shift_y)
            db_utils.update_node_position(cur, node_id=new_id, pipeline_id=target_pipeline_id, position_x=new_position[0], position_y=new_position[1])
    
    return id_map

def delete_node_data(cur, node_ids):
    """
    Delete the data associated with node in list
    """
    if isinstance(node_ids, str):
        node_ids = [node_ids]
    
    for node_id in node_ids:
        # Get the folder path of the node
        folder_path = db_utils.get_node_folder_path(cur, node_id=node_id)
        
        if folder_path and os.path.exists(folder_path):
            # Remove only the contents of the "data" folder, not the folder itself
            data_folder = os.path.join(folder_path, "data")
            if os.path.exists(data_folder):
                for entry in os.scandir(data_folder):
                    if entry.is_file() or entry.is_symlink():
                        os.unlink(entry.path)
                    elif entry.is_dir():
                        shutil.rmtree(entry.path, ignore_errors=True)
            print(f"Data for node {node_id} deleted from {folder_path}")

    for node_id in node_ids:
        db_utils.update_node_status(cur, node_id=node_id, status=NodeState.READY.value)

    else:
        print(f"No data found for node {node_id} or folder does not exist.")

def set_children_stale(cur, pipeline_id, node_id):
    """
    Set the status of all children (descendants) of a node to 'staledata'.
    """
    children = get_all_descendants(cur, pipeline_id, node_id)
    for child_id in children:
        db_utils.update_node_status(cur, node_id=child_id, status=NodeState.STALEDATA.value)

def update_stale_status_for_pipeline_nodes(cur, pipeline_id):
    """
    Propagate 'staledata' status through the pipeline graph:
    If a node has any parent in 'staledata' or 'failed', set its status to 'staledata'.
    This is done recursively using the NetworkX graph structure.
    """
    # Build the graph from the database
    G = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)
    # Traverse in topological order (parents before children)
    for node_id in nx.topological_sort(G):
        # Check all parents
        for parent_id in G.predecessors(node_id):
            parent_status = G.nodes[parent_id]["status"]
            if parent_status in [NodeState.STALEDATA.value, NodeState.FAILED.value, NodeState.READY.value]:
                if G.nodes[node_id]["status"] not in [NodeState.READY.value]:
                    # If the node is not already in 'staledata' or 'failed', update it
                    db_utils.update_node_status(cur, node_id, NodeState.STALEDATA.value)
                    G.nodes[node_id]["status"] = NodeState.STALEDATA.value

def delete_edge_and_update_status(cur, pipeline_id, parent_id, child_id):
    if db_utils.get_pipeline_blocked_status(cur, pipeline_id=pipeline_id):
        raise ValueError(f"Pipeline {pipeline_id} is blocked. Cannot delete edge.")

    # Check if the child node is referenced
    if db_utils.get_node_referenced_status(cur, node_id=child_id):
        raise ValueError(f"Child node {child_id} is referenced. You cannot delete edges from it. Consider duplicating the node if you want to branch the pipeline.")

    # Set all descendants of the child node to 'staledata'
    set_children_stale(cur, pipeline_id, child_id)

    # Remove the edge and update the pipeline using referenced logic
    db_utils.remove_node_relation_with_referenced_logic(cur, parent_id=parent_id, child_id=child_id)

def add_node_relation_safe(cur, pipeline_id, parent_id, child_id):
    """
    Safely add a node relation (edge) to the pipeline graph.
    - Checks that the child node is referenced.
    - Checks that adding the edge does not create a cycle.
    - Adds the relation if all checks pass.
    """
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Get the current pipeline graph
    graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)

    # Check if the child node is referenced
    if db_utils.get_node_referenced_status(cur, node_id=child_id):
        raise ValueError(f"Child node {child_id} is referenced. Cannot add relation.")

    # Check if adding the edge would create a cycle
    temp_graph = graph.copy()
    temp_graph.add_edge(parent_id, child_id)
    if not nx.is_directed_acyclic_graph(temp_graph):
        raise ValueError(f"Adding edge from {parent_id} to {child_id} would create a cycle.")
    
    # If the child node is in 'completed' status, set it to 'staledata'
    child_status = db_utils.get_node_status(cur, node_id=child_id)
    if child_status == NodeState.COMPLETED.value:
        db_utils.update_node_status(cur, node_id=child_id, status=NodeState.STALEDATA.value)

    # Add the relation
    connect_node(cur, child_id=child_id, parent_id=parent_id)

    # Refresh the status of the child node in the pipeline after adding the relation
    update_stale_status_for_pipeline_nodes(cur, pipeline_id)

    return True

def merge_pipelines(cur, source_pipeline_ids):
    """
    Merge multiple pipelines into a new pipeline by copying all their nodes.
    Duplicate nodes are copied only once.
    :param cur: Database cursor
    :param source_pipeline_ids: List of pipeline IDs to merge
    :return: The new target pipeline ID
    """
    # Generate a new pipeline ID
    target_pipeline_id = generate_pip_id()

    # Get project of original pipelines
    original_projects = set()
    for source_pipeline_id in source_pipeline_ids:
        project_id = db_utils.get_project_id_by_pipeline(cur, pipeline_id=source_pipeline_id)
        if project_id:
            original_projects.add(project_id)

    # Check that the project ids are all the same
    if len(original_projects) != 1:
        raise ValueError(f"Cannot merge pipelines from different projects: {original_projects}")

    # Create the new pipeline in the database
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=target_pipeline_id, tag=target_pipeline_id, project_id=original_projects.pop())

    # Keep track of already duplicated nodes
    duplicated_nodes = set()

    # Loop through all source pipelines
    for source_pipeline_id in source_pipeline_ids:
        # Get all node IDs from the source pipeline
        node_ids = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id=source_pipeline_id)
        for node_id in node_ids:
            if node_id not in duplicated_nodes:
                db_utils.duplicate_node_pipeline_relation(cur, source_pipeline_id, node_id, target_pipeline_id)
        duplicated_nodes.update(node_ids)

    # Add pipeline relation for the new pipeline
    for source_pipeline_id in source_pipeline_ids:
        db_utils.add_pipeline_relation(cur, child_id=target_pipeline_id, parent_id=source_pipeline_id)

    return target_pipeline_id


def block_pipeline(cur, pipeline_id):
    db_utils.update_pipeline_blocked_status(cur, pipeline_id=pipeline_id, blocked=True)
    block_all_nodes_in_pipeline(cur, pipeline_id=pipeline_id)

def unblock_pipeline(cur, pipeline_id):
    db_utils.update_pipeline_blocked_status(cur, pipeline_id=pipeline_id, blocked=False)


def detach_node_from_pipeline(cur, pipeline_id, node_id):

    # Check if the node is referenced
    if not db_utils.get_node_referenced_status(cur, node_id=node_id):
        raise ValueError(f"Node {node_id} is not referenced. You can detach only nodes which are referenced.")
    # Check if the node is blocked
    if db_utils.get_node_blocked_status(cur, node_id=node_id):
        raise ValueError(f"Node {node_id} is blocked and cannot be detached.")

    new_node_id = generate_node_id()

    # Duplicate the node with code and the parents relation
    duplicate_node_in_pipeline_w_code_and_data(
        cur, source_pipeline_id=pipeline_id, target_pipeline_id=pipeline_id, 
        source_node_id=node_id, new_node_id=new_node_id, parents=True, childrens=False, withdata=True
    )

    # Remove the original node from the pipeline
    db_utils.remove_node_from_pipeline(cur, pipeline_id=pipeline_id, node_id=node_id)

    # Update update the permissions of the new node
    update_referenced_node_permissions(cur, node_id=node_id)

    return new_node_id

    


def detach_subgraph_from_node(cur, pipeline_id, node_id):
    """
    Detach a subgraph starting from a node in a pipeline.
    :param cur: Database cursor
    :param pipeline_id: ID of the pipeline
    :param node_id: ID of the node to detach the subgraph from

    Return map of detached nodes
    """
    if db_utils.get_node_blocked_status(cur, node_id=node_id):
        raise ValueError(f"Node {node_id} is blocked and cannot be detached.")

    # Get the pipeline graph
    graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)


    # Get all descendants of the node, including the node itself
    subgraph_nodes = set(nx.descendants(graph, node_id))
    subgraph_nodes.add(node_id)

    not_referenced_nodes_in_subgraph = [n for n in subgraph_nodes if not db_utils.get_node_referenced_status(cur, node_id=n)]
    referenced_parents_dict_of_referenced_node = {}

    for node in not_referenced_nodes_in_subgraph:
        # Find direct parents
        parents = list(graph.predecessors(node))
        referenced_parents = [parent for parent in parents if db_utils.get_node_referenced_status(cur, node_id=parent)]
        if referenced_parents:
            referenced_parents_dict_of_referenced_node[node] = referenced_parents

    # Remove from the subthree all the nodes which are in referenced status
    subgraph_nodes = [n for n in subgraph_nodes if n not in not_referenced_nodes_in_subgraph]

    # Duplicate the nodes with their relations
    id_map = duplicate_nodes_in_pipeline_with_relations(
        cur, source_pipeline_id=pipeline_id, target_pipeline_id=pipeline_id,
        source_node_ids=subgraph_nodes, withdata=True
    )

    # Remove the original nodes from the pipeline
    for node in subgraph_nodes:
        db_utils.remove_node_from_pipeline(cur, pipeline_id=pipeline_id, node_id=node)
    
    # Attach not referenced nodes with referenced parents to the new nodes
    for node, parents in referenced_parents_dict_of_referenced_node.items():
        new_parents = [id_map[parent] for parent in parents]
        for new_parent in new_parents:
            # Add the relation to the new node
            connect_node(cur, child_id=node, parent_id=new_parent)
    
    return id_map


def block_node(cur, node_id):
    # Update the blocked status of the node
    db_utils.update_node_blocked_status(cur, node_id=node_id, blocked=True)

    if not db_utils.get_node_referenced_status(cur, node_id=node_id):
        # Remove writing permission
        change_permissions_recursive(
            db_utils.get_node_folder_path(cur, node_id=node_id),
            dir_mode=DIR_CHMOD_BLOCKED, file_mode=FILE_CHMOD_BLOCKED)


def unblock_node(cur, node_id):
    
    # Update the blocked status of the node
    db_utils.update_node_blocked_status(cur, node_id=node_id, blocked=False)

    if not db_utils.get_node_referenced_status(cur, node_id=node_id):
        # Give back writing permission to the group
        change_permissions_recursive(
            db_utils.get_node_folder_path(cur, node_id=node_id),
            dir_mode=DIR_CHMOD_DEFAULT, file_mode=FILE_CHMOD_DEFAULT)

def block_nodes(cur, node_ids):
    """
    Block multiple nodes by updating their blocked status in the database.
    """
    if isinstance(node_ids, str):
        node_ids = [node_ids]

    for node_id in node_ids:
        block_node(cur, node_id)

def block_all_nodes_in_pipeline(cur, pipeline_id):
    """
    Block all nodes in a pipeline by updating their blocked status in the database.
    """
    # Get all nodes in the pipeline
    node_ids = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id=pipeline_id)
    block_nodes(cur, node_ids)

def unblock_all_nodes_in_pipeline(cur, pipeline_id):
    """
    Unblock all nodes in a pipeline by updating their blocked status in the database.
    """
    # Get all nodes in the pipeline
    node_ids = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id=pipeline_id)
    unblock_nodes(cur, node_ids)

def unblock_nodes(cur, node_ids):
    """
    Unblock multiple nodes by updating their blocked status in the database.
    """
    if isinstance(node_ids, str):
        node_ids = [node_ids]

    for node_id in node_ids:
        unblock_node(cur, node_id)

def reference_nodes_into_pipeline(cur, source_pipeline_id, target_pipeline_id, node_ids):
    """
    Reference a node from one pipeline into another.
    - source_pipeline_id: the pipeline ID from which to reference the node
    - target_pipeline_id: the pipeline ID into which to reference the node
    - node_id: the ID of the node to reference
    """
    # Check if the node exists in the source pipeline
    for node_id in node_ids:
        if not db_utils.check_if_node_exists(cur, node_id):
            raise ValueError(f"Node {node_id} does not exist in the source pipeline {source_pipeline_id}.")
        if not can_node_be_referenced(cur, source_pipeline_id, node_id):
            raise ValueError(f"Node {node_id} cannot be referenced from pipeline {source_pipeline_id}. It is not a head of a subgraph. Consider duplicating the node with data first.")

    # Raise an error if the source pipeline is the same as the target pipeline
    if source_pipeline_id == target_pipeline_id:
        raise ValueError("Source pipeline and target pipeline cannot be the same.")
    
    db_utils.duplicate_node_pipeline_relation(cur, source_pipeline_id, node_ids, target_pipeline_id)
    # Update R/W permission for referneced nodes
    update_all_referenced_node_permission_by_pipeline(cur, pipeline_id=target_pipeline_id)


def update_referenced_node_permissions(cur, node_id):
    """
    Update the permissions of a referenced node based on its status.
    If the node is referenced, it should have read-only permissions.
    If the node is not referenced, it should have read-write permissions.
    """
    if db_utils.get_node_referenced_status(cur, node_id=node_id):
        # Node is referenced, set read-only permissions
        change_permissions_recursive(
            db_utils.get_node_folder_path(cur, node_id=node_id),
            dir_mode=DIR_CHMOD_BLOCKED, file_mode=FILE_CHMOD_BLOCKED
        )
    else:
        # Node is not referenced, set read-write permissions
        if not db_utils.get_node_blocked_status(cur, node_id=node_id):
            change_permissions_recursive(
                db_utils.get_node_folder_path(cur, node_id=node_id),
                dir_mode=DIR_CHMOD_DEFAULT, file_mode=FILE_CHMOD_DEFAULT)

def update_all_referenced_node_permission_by_pipeline(cur, pipeline_id):
    """
    Update the permissions of all nodes in a pipeline based on their referenced status.
    """
    # Get all nodes in the pipeline
    node_ids = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id=pipeline_id)
    
    for node_id in node_ids:
        update_referenced_node_permissions(cur, node_id=node_id)

def update_all_referenced_node_permission(cur):
    """
    Update the permissions of all referenced nodes in the database.
    This function iterates through all pipelines and updates the permissions of each node based on its referenced status.
    """
    nodes = db_utils.get_all_node_ids(cur)
    for node_id in nodes:
        update_referenced_node_permissions(cur, node_id=node_id)

def duplicate_pipeline(cur, pipeline_id,  withdata=False):
    """
    Duplicate a pipeline by copying all its nodes and their relations.
    - pipeline_id: the ID of the pipeline to duplicate
    - withdata: if True, also copy the data folder of the nodes
    """
    # Get the original graph from the database
    original_graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)
    project_id = db_utils.get_project_id_by_pipeline(cur, pipeline_id=pipeline_id)

    # Generate a new pipeline ID and tag
    new_pip_id = generate_pip_id()

    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=new_pip_id, tag=new_pip_id, project_id=project_id)    

    # Get all the nodes in the pipeline
    node_ids = list(original_graph.nodes)

    # Duplicate all the node sin the new pipeline including their relations, including the referenced logic
    duplicate_nodes_in_pipeline_with_relations(cur, source_pipeline_id=pipeline_id, target_pipeline_id=new_pip_id, source_node_ids=node_ids, withdata=withdata)

    return new_pip_id

def branch_pipeline(cur, original_pipeline_id, withdata=False):
    # Duplicate the pipeline 
    new_pip_id = duplicate_pipeline(cur, pipeline_id=original_pipeline_id, withdata=withdata)
    db_utils.add_pipeline_relation(cur, child_id=new_pip_id, parent_id=original_pipeline_id)
    return new_pip_id

def create_node_in_pipeline(cur, pipeline_id):
    if db_utils.get_pipeline_blocked_status(cur, pipeline_id=pipeline_id):
        # If the pipeline is blocked, we cannot create a new node
        raise ValueError(f"Pipeline {pipeline_id} is blocked. Cannot create a new node.")

    node_id = generate_node_id()
    folder_path_nodes = os.path.join(os.environ.get("FUSIONPIPE_DATA_PATH"),node_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=False, folder_path=folder_path_nodes)
    position_x = random.randint(-10, 10)
    position_y = random.randint(-10, 10)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, position_x=position_x, position_y=position_y)
    init_node_folder(folder_path_nodes=folder_path_nodes)
    return node_id


def move_pipeline_to_project(cur, pipeline_id, project_id):
    """
    Move a pipeline to a different project by updating its project ID in the database.
    :param cur: Database cursor
    :param pipeline_id: ID of the pipeline to move
    :param project_id: ID of the new project
    """
    # Check if the pipeline exists
    if not db_utils.check_if_pipeline_exists(cur, pipeline_id=pipeline_id):
        raise ValueError(f"Pipeline {pipeline_id} does not exist.")
    
    if db_utils.get_pipeline_blocked_status(cur, pipeline_id=pipeline_id):
        raise ValueError(f"Pipeline {pipeline_id} is blocked. Cannot move it to a different project.")
    
    # Set different project for the pipeline
    db_utils.add_project_to_pipeline(cur, project_id, pipeline_id)

    # Remove the relations of that pipeline
    db_utils.remove_all_pipeline_relation_of_pipeline_id(cur, pipeline_id=pipeline_id)

def generate_edge_id(cur, child_node_id):
    """
    Generate a new 2-digit integer edge ID for a given child node.
    Finds the maximum existing edge ID for the child and returns max + 1.
    """
    existing_edge_ids = db_utils.get_edge_id_of_all_node_parents(cur, child_id=child_node_id)
    # Filter out None and ensure integer conversion
    edge_ids = [int(eid) for eid in existing_edge_ids if eid is not None]
    if edge_ids:
        new_edge_id = max(edge_ids) + 1
    else:
        new_edge_id = 1
    # Ensure it's a 2-digit integer (01, 02, ..., 99)
    return f"{new_edge_id:02d}"

def connect_node(cur, child_id, parent_id):
    edge_id = generate_edge_id(cur, child_node_id=child_id)
    db_utils.add_node_relation(cur, child_id, parent_id, edge_id)
    return edge_id