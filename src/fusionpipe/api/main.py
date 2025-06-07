from fastapi import FastAPI
from pydantic import BaseModel

import yaml
import argparse
import uvicorn
from fusionpipe.api.routes import pipeline_handle
from pathlib import Path
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()
app.include_router(pipeline_handle.router)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Replace with your frontend's URL
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run the FusionPipe FastAPI server.")

    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to run the server on.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to run the server on.",
    )
    args = parser.parse_args()

    uvicorn.run("fusionpipe.api.main:app", host=args.host, port=args.port, reload=True)