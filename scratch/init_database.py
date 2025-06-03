# Initialise databse from a static json file containing the pipeline

import json

path_to_json = "/misc/carpanes/fusionpipe/src/fusionpipe/frontend/tmp/pipeline.json"
path_to_db = "/misc/carpanes/fusionpipe/bin/pipeline.db"

with open(path_to_json, "r") as f:
    data = json.load(f)


from fusionpipe.utils import db_utils
from fusionpipe.utils import pip_utils


conn = db_utils.load_db(path_to_db)
cur = conn.cursor()

pip_utils.graph_dict_to_db(data, cur)

conn.commit()
