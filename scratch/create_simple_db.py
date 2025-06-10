from fusionpipe.utils.db_utils import create_db
from fusionpipe.utils.db_utils import add_pipeline

path_to_db = "/home/cisko90/fusionpipe/bin/nodes/connection.db"
#path_to_db = r"C:\Users\franc\Documents\fusionpipe\bin\connection.db"
path_to_db = "/misc/carpanes/fusionpipe/bin/pipeline.db"

# Create a simple database
conn = create_db(path_to_db)
print(f"Database created at {path_to_db}")
conn.close()


# Add a simple pipeline
conn = create_db(path_to_db)
cur = conn.cursor()
pipeline_id = "simple_pipeline"
add_pipeline(cur, pipeline_id=pipeline_id, tag="v1.0", owner="user1", notes="This is a simple pipeline.")
conn.commit()
print(f"Pipeline '{pipeline_id}' added to the database.")
conn.close()

from fusionpipe.utils.db_utils import add_node_to_nodes
from fusionpipe.utils.db_utils import add_node_to_pipeline, add_node_relation

# Add some nodes to the database
conn = create_db(path_to_db)
cur = conn.cursor()

nodes = [
    {"node_id": "node1", "status": "ready", "editable": True, "notes": "First node","folder_path": "dummy1"},
    {"node_id": "node2", "status": "ready", "editable": True, "notes": "Second node", "folder_path": "dummy2"},
    {"node_id": "node3", "status": "ready", "editable": False, "notes": "Third node", "folder_path": "dummy3"},
]

for node in nodes:
    add_node_to_nodes(cur, node_id=node["node_id"], status=node["status"], editable=node["editable"], notes=node["notes"], folder_path=node["folder_path"])

add_node_relation(cur, child_id="node1", parent_id="node2")


pipeline_id = "simple_pipeline"
node_ids = ["node1", "node2", "node3"]

for node_id in node_ids:
    add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="user1")

conn.commit()
print(f"Nodes added to pipeline '{pipeline_id}'.")
conn.close()