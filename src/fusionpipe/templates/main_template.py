# This script provides example on how to run a python script, matlab script, python notebook
# Warning you must set up the following enviroment variable to get access to the database unless not already set in your .bash_profile:
# export DATABASE_URL="dbname=<yourdb> port=<port>" 
import os
from datetime import datetime
import sys
sys.path.insert(0, os.environ.get("USER_UTILS_FOLDER_PATH"))
from python_user_utils.node_api import get_all_parent_node_folder_paths, get_node_id, get_folder_path_data, get_folder_path_reports, get_folder_path_code, get_folder_path_node


if __name__ == "__main__":
    print("Running main template.py")
