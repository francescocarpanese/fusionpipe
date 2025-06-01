
# Simple test to check the serialisation is done correctly

import os
from fusionpipe.utils import db_utils
from fusionpipe.utils.pip_utils import (
    NodeState,
    graph_to_db,
    db_to_graph_from_pip_id,
    generate_node_id,
    generate_pip_id,
    visualize_pip,
)
import networkx as nx

# Settings
db_path = "demo_pipeline.db"
if os.path.exists(db_path):
    os.remove(db_path)
conn = db_utils.init_db(db_path)
cur = conn.cursor()

# 1. Generate a simple DAG
G = nx.DiGraph()
G.add_edges_from([
    ("A", "B"),
    ("A", "C"),
    ("C", "D"),
])
G.graph['id'] = generate_pip_id()
G.graph['tag'] = "demo_tag"

# Set node status
for node in G.nodes:
    G.nodes[node]['status'] = NodeState.READY.value

# 2. Visualize the graph
print("Visualizing original graph...")
visualize_pip(G)

graph_to_db(G, cur)
conn.commit()

# 4. Load the graph from the database
print("Loading graph from database...")
G_loaded = db_to_graph_from_pip_id(cur, G.graph['id'])

# 5. Visualize the loaded graph
print("Visualizing loaded graph...")
visualize_pip(G_loaded)

conn.close()
