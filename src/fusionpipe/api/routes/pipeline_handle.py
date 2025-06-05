
from fastapi import APIRouter, Depends, HTTPException
import os
from fastapi.responses import JSONResponse


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

@router.get("/get_all_pipeline_ids")
def get_pip_ids(db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        pip_ids = db_utils.get_all_pipeline_ids(cur)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    if not pip_ids:
        pip_ids = []
    
    return {"pip_ids": pip_ids}

@router.get("/get_pipeline/{pipeline_id}")
def get_pipeline(pipeline_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        # Fetch the pipeline and convert it to a dictionary
        pipeline_dict = pip_utils.db_to_graph_dict_from_pip_id(cur, pipeline_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return JSONResponse(
        content=pipeline_dict,
        headers={"Cache-Control": "no-store"}
    )

@router.post("/create_node_in_pipeline/{pipeline_id}")
def add_node_to_pipeline(pipeline_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        node_id = pip_utils.generate_node_id()
        db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", editable=True)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"message": f"Node {node_id} added to pipeline {pipeline_id}"}

@router.delete("/delete_node_from_pipeline/{pipeline_id}/{node_id}")
def delete_node_from_pipeline(pipeline_id: str, node_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        db_utils.remove_node_from_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"message": f"Node {node_id} deleted from pipeline {pipeline_id}"}