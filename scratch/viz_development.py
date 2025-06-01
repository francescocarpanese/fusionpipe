from fusionpipe.utils.pip_utils import visualize_pip_static, visualize_pip_interactive

def dag_dummy_1():
    import networkx as nx
    # Create a simple directed acyclic graph (DAG) for testing
    G = nx.DiGraph()
    G.add_edges_from([
        ("A", "B"),
        ("A", "C"),
        ("C", "D"),
    ])
    G.name = "12345"
    G.graph['pipeline_id'] = G.name
    G.graph['notes'] = "A simple test DAG"
    G.graph['tag'] = "test_tag"
    G.graph['owner'] = "test_group"

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['editable'] = True
        G.nodes[node]['tag'] = 'test_tag'
        G.nodes[node]['notes'] = 'test notes'
        if node == "A":
            G.nodes[node]['status'] = "ready"
        elif node == "B":
            G.nodes[node]['status'] = "running"
        elif node == "C":
            G.nodes[node]['status'] = "completed"
        elif node == "D":
            G.nodes[node]['status'] = "staledata"

    return G


dag = dag_dummy_1()
# Visualize the graph using the static visualization function
visualize_pip_static(dag)    

visualize_pip_interactive(dag)  # This will open an interactive visualization in a web browser