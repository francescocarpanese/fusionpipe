import os
import sys
import psycopg2
from fusionpipe.utils import db_utils


new_base_path = '/data/defuse/fusionpipe-data-1'
old_base_path = '/data/defuse/fusionpipe-data'
project_id = 'pr_20250812085600_8056'

db_url=os.environ.get("DATABASE_URL")
dry_run = False
#db_url="dbname=fusionpipe_prod4 port=5432"

conn = psycopg2.connect(db_url)
cur = conn.cursor()

# Get nodes
nodes = db_utils.get_all_nodes_from_nodes(cur)
nodes_wo_pipeline = db_utils.get_nodes_without_pipeline(cur)
node_wo_project = db_utils.get_nodes_without_project(cur)

# Build nodes dict with 
# node_id:
#   - project_id
#   - old_folder_path
#   - new_folder_path
# In this migration the new folder path was moving to flat structure to project_id/node_id
nodes_dict = {}
for i in range(len(nodes)):
    project_id = db_utils.get_project_id_by_node(cur, nodes[i])
    if project_id:
        print(f"Node {nodes[i]} belongs to project {project_id}")
        nodes_dict[nodes[i]] = {
            'project_id': project_id,
            'new_folder_path': db_utils.get_node_folder_path(cur, nodes[i]),
        }
        list_path = nodes_dict[nodes[i]]['new_folder_path'].split('/')
        nodes_dict[nodes[i]]['old_folder_path'] = old_base_path + '/' +  nodes[i]
        nodes_dict[nodes[i]]['new_folder_path'] = new_base_path + '/' + project_id + '/'  +  nodes[i]

project_list  = list(set([v['project_id'] for x,v in nodes_dict.items()]))   

for i in range(len(project_list)):
    project_id = project_list[i]
    project_path = new_base_path + '/' + project_id
    if not os.path.exists(project_path):
        os.makedirs(project_path)
        print(f"Created project directory: {project_path}")

def ignore_venv(dir, files):
    # Ignore any directory or file that starts with .venv
    return [f for f in files if '.venv' in f]

# Copy the nodes to the new destination
from tqdm import tqdm
import subprocess
for k, v in tqdm(nodes_dict.items()):
    old_path = v['old_folder_path']
    new_path = v['new_folder_path']
    if v['project_id'] == project_id:
        if os.path.exists(old_path):
            try:
                if not dry_run:
                    subprocess.run(
                        ['cp', '-r', old_path, new_path],
                        check=True
                    )
                print(f"Copied node directory from {old_path} to {new_path}")
            except subprocess.CalledProcessError as e:
                print(f"Failed to copy {old_path} to {new_path}: {e}")
        else:
            print(f"Old path does not exist: {old_path}")