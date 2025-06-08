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
        pip_utils.delete_node_from_pipeline_with_editable_logic(cur, node_id=node_id, pipeline_id=pipeline_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"message": f"Node {node_id} deleted from pipeline {pipeline_id}"}

@router.delete("/delete_edge/{pipeline_id}/{source}/{target}")
def delete_edge(pipeline_id: str, source: str, target: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        db_utils.remove_node_relation_with_editable_logic(cur, parent_id=source, child_id=target)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Edge {source} -> {target} deleted from pipeline {pipeline_id}"}

@router.post("/connect_nodes")
async def connect_nodes_in_pipeline(payload: dict, db_conn=Depends(get_db)):
    source = payload.get("source")
    target = payload.get("target")
    if not source or not target:
        raise HTTPException(status_code=400, detail="Missing source or target node id")
    cur = db_conn.cursor()
    try:
        db_utils.add_node_relation(cur, parent_id=source, child_id=target)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Connected node {source} to node {target}"}

@router.delete("/delete_pipeline/{pipeline_id}")
def delete_pipeline(pipeline_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        db_utils.remove_pipeline_from_everywhere(cur, pipeline_id=pipeline_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Pipeline {pipeline_id} deleted successfully"}

@router.get("/branch_pipeline/{pipeline_id}/{start_node_id}")
def branch_pipeline(pipeline_id: str, start_node_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        # Fetch the pipeline graph starting from the given node
        new_pipeline = pip_utils.branch_pipeline_from_node(cur, pipeline_id, start_node_id)
        db_conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"pipeline_id": pipeline_id, "start_node_id": start_node_id, "new_pipeline": new_pipeline}

@router.post("/update_node_tag/{pipeline_id}/{node_id}")
def update_node_tag(pipeline_id: str, node_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    node_tag = payload.get("node_tag")
    if node_tag is None:
        raise HTTPException(status_code=400, detail="Missing node_tag")
    try:
        db_utils.update_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, node_tag=node_tag)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Node tag updated for node {node_id} in pipeline {pipeline_id}"}

@router.post("/update_node_notes/{pipeline_id}/{node_id}")
def update_node_notes(pipeline_id: str, node_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    notes = payload.get("notes")
    if notes is None:
        raise HTTPException(status_code=400, detail="Missing notes")
    try:
        db_utils.update_node_notes(cur, node_id=node_id, notes=notes)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Node notes updated for node {node_id} in pipeline {pipeline_id}"}