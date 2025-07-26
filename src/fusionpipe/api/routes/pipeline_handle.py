from fastapi import APIRouter, Depends, HTTPException
import os
from fastapi.responses import JSONResponse


from fusionpipe.utils import db_utils, pip_utils, runner_utils
import random

router = APIRouter()

def get_db():
    db_path = os.environ.get("DATABASE_URL")
    db = db_utils.connect_to_db(db_path)
    try:
        yield db
    finally:
        db.close()

@router.post("/create_pipeline")
def create_pipeline(payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    pipeline_id = pip_utils.generate_pip_id()
    project_id = payload.get("project_id")
    try:
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=None, project_id=project_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"pipeline_id": pipeline_id, "message": "Pipeline created"}


@router.post("/create_project")
def create_project(db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    project_id = pip_utils.generate_project_id()
    try:
        db_utils.add_project(cur, project_id=project_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"project_id": project_id, "message": "Project created"}

@router.get("/get_all_pipeline_ids_tags_dict")
def get_pip_ids(db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        ids_tags_dict = db_utils.get_all_pipeline_ids_tags_dict(cur)
        db_conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    if not ids_tags_dict:
        ids_tags_dict = {}
    
    return ids_tags_dict

@router.get("/get_all_project_ids_tags_dict")
def get_project_ids(db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        ids_tags_dict = db_utils.get_all_project_ids_tags_dict(cur)
        db_conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    if not ids_tags_dict:
        ids_tags_dict = {}
    
    return ids_tags_dict

@router.get("/get_pipeline/{pipeline_id}")
def get_pipeline(pipeline_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        # Fetch the pipeline and convert it to a dictionary
        pipeline_dict = pip_utils.db_to_pipeline_dict_from_pip_id(cur, pipeline_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return JSONResponse(
        content=pipeline_dict,
        headers={"Cache-Control": "no-store"}
    )

@router.get("/get_project/{project_id}")
def get_project(project_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        project_dict = pip_utils.db_to_project_dict_from_project_id(cur, project_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return JSONResponse(
        content=project_dict,
        headers={"Cache-Control": "no-store"}
    )

@router.post("/create_node_in_pipeline/{pipeline_id}")
def add_node_to_pipeline(pipeline_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        node_id = pip_utils.generate_node_id()
        folder_path_nodes = os.path.join(os.environ.get("FUSIONPIPE_DATA_PATH"),node_id)
        db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", editable=True, folder_path=folder_path_nodes)
        position_x = random.randint(-10, 10)
        position_y = random.randint(-10, 10)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, position_x=position_x, position_y=position_y)
        pip_utils.init_node_folder(folder_path_nodes=folder_path_nodes)
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
        pip_utils.delete_edge_and_update_status(cur, pipeline_id=pipeline_id, parent_id=source, child_id=target)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Edge {source} -> {target} deleted from pipeline {pipeline_id}"}

@router.post("/connect_nodes")
async def connect_nodes_in_pipeline(payload: dict, db_conn=Depends(get_db)):
    source = payload.get("source")
    target = payload.get("target")
    pipeline_id = payload.get("pipelineId")
    if not source or not target:
        raise HTTPException(status_code=400, detail="Missing source or target node id")
    cur = db_conn.cursor()
    try:
        pip_utils.add_node_relation_safe(cur, pipeline_id=pipeline_id, parent_id=source, child_id=target)
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

@router.delete("/delete_project/{project_id}")
def delete_project(project_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        db_utils.remove_project_from_everywhere(cur, project_id=project_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Project {project_id} deleted successfully"}

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
        db_utils.update_node_tag(cur, node_id=node_id, node_tag=node_tag)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Node tag updated for node {node_id} in pipeline"}

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

@router.post("/update_pipeline_tag/{pipeline_id}")
def update_pipeline_tag(pipeline_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    tag = payload.get("tag")
    if tag is None:
        raise HTTPException(status_code=400, detail="Missing tag")
    try:
        db_utils.update_pipeline_tag(cur, pipeline_id=pipeline_id, tag=tag)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Pipeline tag updated for pipeline {pipeline_id}"}

@router.post("/update_project_tag/{project_id}")
def update_project_tag(project_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    tag = payload.get("tag")
    if tag is None:
        raise HTTPException(status_code=400, detail="Missing tag")
    try:
        db_utils.update_project_tag(cur, project_id=project_id, tag=tag)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Project tag updated for project {project_id}"}

@router.post("/update_project_notes/{project_id}")
def update_project_notes(project_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    notes = payload.get("notes")
    if notes is None:
        raise HTTPException(status_code=400, detail="Missing notes")
    try:
        db_utils.update_project_notes(cur, project_id=project_id, notes=notes)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Project notes updated for project {project_id}"}


@router.post("/update_pipeline_notes/{pipeline_id}")
def update_pipeline_notes(pipeline_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    notes = payload.get("notes")
    if notes is None:
        raise HTTPException(status_code=400, detail="Missing notes")
    try:
        db_utils.update_pipeline_notes(cur, pipeline_id=pipeline_id, notes=notes)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Pipeline notes updated for pipeline {pipeline_id}"}

@router.post("/update_node_position/{pipeline_id}")
def update_node_position(pipeline_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    nodes = payload.get("nodes")
    if not nodes:
        raise HTTPException(status_code=400, detail="Missing nodes data")
    
    try:
        for node in nodes:
            node_id = node.get("id")
            position = node.get("position")
            
            if node_id and position and "x" in position and "y" in position:
                db_utils.update_node_position(cur, 
                                            node_id=node_id, 
                                            pipeline_id=pipeline_id, 
                                            position_x=position["x"], 
                                            position_y=position["y"])
        
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"message": f"Node positions updated in pipeline {pipeline_id}"}

@router.post("/update_node_status/{node_id}")
def update_node_status(node_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    status = payload.get("status")
    if status is None:
        raise HTTPException(status_code=400, detail="Missing status")
    try:
        db_utils.update_node_status(cur, node_id=node_id, status=status)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Node status updated for node {node_id}"}

@router.post("/run_pipeline/{pipeline_id}")
def run_pipeline_route(pipeline_id: str, payload: dict = None, db_conn=Depends(get_db)):
    poll_interval = 1.0
    debug = False

    if payload:
        poll_interval = payload.get("poll_interval", 1.0)
        debug = payload.get("debug", False)

    try:
        result = runner_utils.run_pipeline(
            db_conn,
            pipeline_id,
            poll_interval=poll_interval,
            debug=debug
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Pipeline {pipeline_id} run completed", "result": result}

@router.post("/run_pipeline_up_to_node/{pipeline_id}/{node_id}")
def run_pipeline_up_to_node_route(pipeline_id: str, node_id: str, payload: dict = None, db_conn=Depends(get_db)):
    run_mode = "local"
    poll_interval = 1.0
    debug = False

    if payload:
        poll_interval = payload.get("poll_interval", 1.0)
        debug = payload.get("debug", False)

    try:
        result = runner_utils.run_pipeline(
            db_conn,
            pipeline_id,
            last_node_id=node_id,
            poll_interval=poll_interval,
            debug=debug
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Pipeline {pipeline_id} run up to node {node_id} completed", "result": result}

@router.post("/run_node/{node_id}")
def run_node_route(node_id: str, payload: dict = None, db_conn=Depends(get_db)):
    try:
        runner_utils.run_node(db_conn, node_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Node {node_id} run completed"}

@router.post("/duplicate_nodes_in_pipeline")
def duplicate_nodes_in_pipeline_route(payload: dict, db_conn=Depends(get_db)):
    """
    Duplicate a set of nodes from a source pipeline into a target pipeline, creating new node id, preserving their relations.
    Payload example: {"source_pipeline_id": "...", "target_pipeline_id": "...", "node_ids": ["n_123", "n_456"]}
    """
    cur = db_conn.cursor()
    source_pipeline_id = payload.get("source_pipeline_id")
    target_pipeline_id = payload.get("target_pipeline_id")
    withdata = payload.get("withdata", False)
    node_ids = payload.get("node_ids")
    if not source_pipeline_id or not target_pipeline_id or not node_ids or not isinstance(node_ids, list):
        raise HTTPException(status_code=400, detail="Payload must contain source_pipeline_id, target_pipeline_id, and a list of node_ids")
    try:
        id_map = pip_utils.duplicate_nodes_in_pipeline_with_relations(
            cur, source_pipeline_id, target_pipeline_id, node_ids, withdata=withdata
        )
        for new_node_id in id_map.values():
            if withdata:
              db_utils.update_node_status(cur, node_id=new_node_id, status="staledata")
            else:
              db_utils.update_node_status(cur, node_id=new_node_id, status="ready")
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "message": f"Nodes {node_ids} duplicated from pipeline {source_pipeline_id} to {target_pipeline_id} with code, data, and relations",
        "id_map": id_map
    }

@router.post("/reference_nodes_into_pipeline")
def reference_nodes_into_pipeline_route(payload: dict, db_conn=Depends(get_db)):

    cur = db_conn.cursor()
    source_pipeline_id = payload.get("source_pipeline_id")
    target_pipeline_id = payload.get("target_pipeline_id")
    node_ids = payload.get("node_ids")
    if not source_pipeline_id or not target_pipeline_id or not node_ids or not isinstance(node_ids, list):
        raise HTTPException(status_code=400, detail="Payload must contain source_pipeline_id, target_pipeline_id, and a list of node_ids")
    try:
        id_map = db_utils.duplicate_node_pipeline_relation(
            cur, source_pipeline_id, node_ids, target_pipeline_id
        )
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "message": f"Nodes {node_ids} duplicated from pipeline {source_pipeline_id} to {target_pipeline_id} with code, data, and relations",
        "id_map": id_map
    }

@router.delete("/delete_node_data")
def delete_node_data_route(payload: dict, db_conn=Depends(get_db)):
    """
    Delete the data associated with a list of nodes (but not the nodes themselves).
    Payload example: {"node_ids": ["n_123", "n_456"]}
    """
    node_ids = payload.get("node_ids")
    pipeline_id = payload.get("pipeline_id")
    if not node_ids or not isinstance(node_ids, list):
        raise HTTPException(status_code=400, detail="Payload must contain a list of node_ids")
    try:
        cur = db_conn.cursor()
        pip_utils.delete_node_data(cur, node_ids)
        for node_id in node_ids:
            pip_utils.update_stale_status_for_pipeline_nodes(cur, pipeline_id=pipeline_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Data for nodes {node_ids} deleted successfully"}

@router.post("/manual_set_node_status/{node_id}/{status}")
def manual_set_node_status(node_id: str, status: str, payload: dict = None, db_conn=Depends(get_db)):
    """
    Manually set the status of a node.
    Expects payload: {"pipeline_id": "..."}
    """
    if status not in ["ready", "running", "completed", "failed", "staledata"]:
        raise HTTPException(status_code=400, detail="Invalid status")
    
    pipeline_id = None
    if payload:
        pipeline_id = payload.get("pipeline_id")
    if not pipeline_id:
        raise HTTPException(status_code=400, detail="Missing pipeline_id in payload")
    
    cur = db_conn.cursor()
    try:
        db_utils.update_node_status(cur, node_id=node_id, status=status)
        pip_utils.update_stale_status_for_pipeline_nodes(cur, pipeline_id=pipeline_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"message": f"Node {node_id} status set to {status} and stale status updated for pipeline {pipeline_id}"}

@router.post("/kill_node/{node_id}")
def kill_node_route(node_id: str, db_conn=Depends(get_db)):
    try:
        runner_utils.kill_running_process(db_conn, node_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"Kill signal sent to node {node_id} (if running)"}

@router.post("/move_pipeline_to_project")
def add_pipeline_to_project_route(payload: dict, db_conn=Depends(get_db)):
    """
    Associate a pipeline with a project.
    Payload example: {"project_id": "...", "pipeline_id": "..."}
    """
    project_id = payload.get("project_id")
    pipeline_id = payload.get("pipeline_id")
    if not project_id or not pipeline_id:
        raise HTTPException(status_code=400, detail="Missing project_id or pipeline_id")
    cur = db_conn.cursor()
    try:
        rows_affected = db_utils.add_project_to_pipeline(cur, project_id, pipeline_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "message": f"Pipeline {pipeline_id} added to project {project_id}",
        "rows_affected": rows_affected
    }

@router.get("/get_pipelines_in_project/{project_id}")
def get_pipelines_in_project(project_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        pipelines = db_utils.get_pipeline_ids_by_project(cur, project_id)
        db_conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    if not pipelines:
        return {"message": f"No pipelines found in project {project_id}"}
    
    return {"pipelines": pipelines}

@router.post("/merge_pipelines")
def merge_pipelines_route(payload: dict, db_conn=Depends(get_db)):
    """
    Merge multiple pipelines into a new pipeline by copying all their nodes.
    Payload example: {"source_pipeline_ids": ["pipeline_1", "pipeline_2"]}
    """
    source_pipeline_ids = payload.get("source_pipeline_ids")
    if not source_pipeline_ids or not isinstance(source_pipeline_ids, list):
        raise HTTPException(status_code=400, detail="Payload must contain a list of source_pipeline_ids")
    
    cur = db_conn.cursor()
    try:
        target_pipeline_id = pip_utils.merge_pipelines(cur, source_pipeline_ids)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"message": f"Pipelines {source_pipeline_ids} merged into new pipeline {target_pipeline_id}", "target_pipeline_id": target_pipeline_id}

@router.get("/get_node_parameters_yaml/{node_id}")
def get_node_parameters_yaml(node_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    folder_path = None
    try:
        folder_path = db_utils.get_node_folder_path(cur, node_id=node_id)
        if not folder_path:
            raise HTTPException(status_code=404, detail="Node folder path not found")
        yaml_path = os.path.join(folder_path, "code", "node_parameters.yaml")
        if not os.path.exists(yaml_path):
            raise HTTPException(status_code=404, detail="node_parameters.yaml not found")
        with open(yaml_path, "r") as f:
            content = f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"content": content}

@router.post("/update_node_parameters_yaml/{node_id}")
def update_node_parameters_yaml(node_id: str, payload: dict, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    folder_path = None
    try:
        folder_path = db_utils.get_node_folder_path(cur, node_id=node_id)
        if not folder_path:
            raise HTTPException(status_code=404, detail="Node folder path not found")
        yaml_path = os.path.join(folder_path, "code", "node_parameters.yaml")
        content = payload.get("content")
        if content is None:
            raise HTTPException(status_code=400, detail="Missing content")
        with open(yaml_path, "w") as f:
            f.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": f"node_parameters.yaml updated for node {node_id}"}


@router.post("/detach_subgraph_from_node/{pipeline_id}/{node_id}")
def detach_subgraph_from_node(pipeline_id: str, node_id: str, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    try:
        pip_utils.detach_subgraph_from_node(cur, pipeline_id=pipeline_id, node_id=node_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"message": f"Node {node_id} detached from pipeline {pipeline_id}"}