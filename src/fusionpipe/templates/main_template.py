# This script provides example on how to run a python script, matlab script, python notebook
# Warning you must set up the following enviroment variable to get access to the database unless not already set in your .bash_profile:
# export DATABASE_URL="dbname=<yourdb> port=<port>" 
import os
from datetime import datetime
import sys
from fp_user_utils.user_api import get_current_node_id, get_info_parents

if __name__ == "__main__":
    print("Running main template.py")

