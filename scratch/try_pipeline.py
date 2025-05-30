import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime
import random
from fusionpipe.utils.pip_utils import (
    generate_id,
    create_node_folder,
    create_pipeline_file,
    check_node_exist,
    load_pipeline_from_file,
    add_node,
    generate_data_folder_structure
)

import os
import json

%load_ext autoreload
%autoreload 2


base_path = "/misc/carpanes/fusionpipe/bin"

generate_data_folder_structure(base_path)

settings = {
    "pipeline_folder": "/home/cisko90/fusionpipe/bin/pipelines",
    "node_folder": "/misc/carpanes/fusionpipe/bin/pipelines",
}


node_id = create_node_folder(settings, verbose=True)

node_id = "20250529145832_3819"

pip_id = create_pipeline_file(settings, verbose=True)

g1 = load_pipeline_from_file(settings, pip_id=pip_id)
g1 = add_node(settings, g1, node_id)

add_node(settings, g1, node_id)


# Managing pipeline
g1 = DAG(settings, pip_id=pip_id)
g1.load_pipeline_from_file()
g1.visualize_pip()
g1.add_node(node_id)
g1.connect_node_to_parent("20250529184136_6859", "20250529145832_3819")
g1.connect_node_to_parent("20250529184039_9017", "20250529145832_3819")
g1.detach_node("20250529184039_9017")
g1.remove_input_edges("20250529145832_3819")
g1.save_pipeline()

