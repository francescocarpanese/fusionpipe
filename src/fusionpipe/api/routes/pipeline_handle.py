
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