from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from fusionpipe.utils import pipeline_db, pip_utils
import os
from contextlib import asynccontextmanager

app = FastAPI()

DB_PATH = "/home/cisko90/fusionpipe/bin/connection.db"

def get_db():
    conn = pipeline_db.load_db(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

class AddNodetoPipelineRequest(BaseModel):
    pipeline_id: str

@app.post("/create_pipeline")
def create_pipeline(db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    pipeline_id = pip_utils.generate_pip_id()
    try:
        pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag=None)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"pipeline_id": pipeline_id, "message": "Pipeline created"}

@app.post("/add_node")
def add_node(req: AddNodetoPipelineRequest, db_conn=Depends(get_db)):
    cur = db_conn.cursor()
    if not pipeline_db.check_pipeline_exists(cur, req.pipeline_id):
        raise HTTPException(status_code=404, detail="Pipeline not found")
    node_id = pip_utils.generate_node_id()
    try:
        pipeline_db.add_node_to_nodes(cur, node_id=node_id)
        pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=req.pipeline_id)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    return {"node_id": node_id, "message": "Node added to pipeline"}

# Optional: Enable CORS for local frontend dev
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)