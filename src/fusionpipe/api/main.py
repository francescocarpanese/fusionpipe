from fastapi import FastAPI
from pydantic import BaseModel
import os

import yaml
import argparse
import uvicorn
from fusionpipe.api.routes import pipeline_handle
from pathlib import Path
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()
app.include_router(pipeline_handle.router)

# Add CORS middleware
frontend_host = os.getenv("FRONTEND_HOST", "localhost")
frontend_port = os.getenv("FRONTEND_PORT", "3000")
frontend_url = f"http://{frontend_host}:{frontend_port}"

app.add_middleware(
    CORSMiddleware,
    allow_origins=[frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run the FusionPipe FastAPI server.")

    default_port = os.getenv("BACKEND_PORT", 8000)
    default_host = os.getenv("BACKEND_HOST", "127.0.0.1")

    parser.add_argument(
        "--host",
        type=str,
        default=default_host,
        help="Host to run the server on.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=default_port,
        help="Port to run the server on.",
    )
    args = parser.parse_args()

    uvicorn.run("fusionpipe.api.main:app", host=args.host, port=args.port, reload=True)