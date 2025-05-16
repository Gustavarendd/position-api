# app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import os

app = FastAPI()

# File output path
FILE_OUTPUT_PATH = "/data/positions/position.txt"

class InputData(BaseModel):
    lon: float
    lat: float
    time: str
    speed: float

@app.post("/submit/")
async def submit_data(data: InputData):
    try:
        # Create content
        content = (
            f"Longitude: {data.lon}\n"
            f"Latitude: {data.lat}\n"
            f"Time: {data.time}\n"
            f"Speed: {data.speed}\n"
        )

        # Ensure directory exists
        os.makedirs(os.path.dirname(FILE_OUTPUT_PATH), exist_ok=True)

        # Save file
        with open(FILE_OUTPUT_PATH, 'w') as f:
            f.write(content)

        return {"message": f"Data written to {FILE_OUTPUT_PATH}"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
