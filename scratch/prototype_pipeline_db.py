from  fusionpipe.utils import pipeline_db
from fusionpipe.utils.pip_utils import generate_pip_id, generate_node_id

%load_ext autoreload
%autoreload 2

# Test call
path_to_db = "/misc/carpanes/fusionpipe/scratch/pipeline.db"
conn = pipeline_db.load_db(path_to_db)
cur = pipeline_db.init_graph_db(conn)

pip_id = generate_pip_id()
node_id = generate_node_id()

pipeline_db.add_pipeline(cur, pipeline_id=pip_id, tag="test_pipeline")
conn.commit()

pipeline_db.add_node(cur, node_id=node_id)
conn.commit()




# TODO
# Bring this to a test
# Load pipelin from db
# write pipeline to db?
# Connect node 
# Tag node 
# Duplicate pipeline
# Remove node from pipeline
# Find all disconected nodes
# Prune nodes
