
from fastapi import APIRouter, Depends, HTTPException
import os

from fusionpipe.utils import db_utils, pip_utils

router = APIRouter()

def get_db():
    db_path = os.environ.get("FUSIONPIPE_DB_PATH")
    db = db_utils.connect_to_db(db_path)
    try:
        yield db
    finally:
        db.close()


@router.post("/create_pipeline")
def create_pipeline(db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    pipeline_id = pip_utils.generate_pip_id()
    try:
        db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag=None)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"pipeline_id": pipeline_id, "message": "Pipeline created"}

# @router.post("/add_node")
# def add_node(req: AddNodetoPipelineRequest, db_conn=Depends(get_db)):
#     cur = db_conn.cursor()
#     if not db_utils.check_pipeline_exists(cur, req.pipeline_id):
#         raise HTTPException(status_code=404, detail="Pipeline not found")
#     node_id = pip_utils.generate_node_id()
#     try:
#         db_utils.add_node_to_nodes(cur, node_id=node_id)
#         db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=req.pipeline_id)
#         db_conn.commit()
#     except Exception as e:
#         db_conn.rollback()
#         raise HTTPException(status_code=500, detail=str(e))
#     return {"node_id": node_id, "message": "Node added to pipeline"}
