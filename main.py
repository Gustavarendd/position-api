import json
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import storage
from typing import List
import os
import logging
import httpx
from datetime import datetime, timedelta, timezone

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https://.*\.amphitrite\.fr|http://localhost:5173",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BUCKET_NAME = "position-api"
POSITION_FILE_NAME = "positions/position.txt"
FUEL_CONSUMPTION_FILE_NAME = "fuel-consumption/ship_speed_consumption.json"
SINGAPORE_CACHE_FOLDER = "singapore-cache/"  # Folder for caching Singapore API data

# Singapore API configuration
SINGAPORE_API_URL = os.getenv("SINGAPORE_API_URL", "https://sg-mdh-api.mpa.gov.sg/v1/vessel/positions/snapshot")  # Replace with actual API URL

CACHE_EXPIRY_MINUTES = 10  # Cache expiry time in minutes

class InputData(BaseModel):
    lon: float
    lat: float
    time: str

class VesselParticulars(BaseModel):
    vesselName: str
    callSign: str
    imoNumber: str
    flag: str
    vesselLength: float
    vesselBreadth: float
    vesselDepth: float
    vesselType: str
    grossTonnage: float
    netTonnage: float
    deadweight: float
    mmsiNumber: str
    yearBuilt: str

class SingaporeApiResponse(BaseModel):
    vesselParticulars: VesselParticulars
    latitude: float
    longitude: float
    latitudeDegrees: float
    longitudeDegrees: float
    speed: float
    course: float
    heading: float
    timeStamp: str

def get_gcs_client():
    return storage.Client()


def get_latest_cache_file():
    """
    Get the most recent cache file from GCS that's within the expiry time.
    Returns (blob, timestamp) if valid cache exists, (None, None) otherwise.
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        
        # List all blobs in the singapore-cache folder
        blobs = bucket.list_blobs(prefix=SINGAPORE_CACHE_FOLDER)
        
        valid_blobs = []
        current_time = datetime.now(timezone.utc)
        
        for blob in blobs:
            # Extract timestamp from filename (format: singapore-cache/YYYY-MM-DD_HH-MM-SS.json)
            if blob.name.endswith('.json'):
                try:
                    filename = blob.name.replace(SINGAPORE_CACHE_FOLDER, '').replace('.json', '')
                    file_timestamp = datetime.strptime(filename, '%Y-%m-%d_%H-%M-%S').replace(tzinfo=timezone.utc)

                    
                    # Check if file is within expiry time
                    if current_time - file_timestamp <= timedelta(minutes=CACHE_EXPIRY_MINUTES):
                        valid_blobs.append((blob, file_timestamp))
                except ValueError:
                    # Skip files with invalid timestamp format
                    continue
        
        if valid_blobs:
            # Return the most recent valid cache file
            latest_blob, latest_timestamp = max(valid_blobs, key=lambda x: x[1])
            return latest_blob, latest_timestamp
        
        return None, None
    except Exception as e:
        logging.warning(f"Error checking cache files: {e}")
        return None, None


def save_to_cache(data):
    """
    Save data to GCS with timestamp-based filename.
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        
        # Create filename with current timestamp
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
        cache_filename = f"{SINGAPORE_CACHE_FOLDER}{timestamp}.json"
        
        blob = bucket.blob(cache_filename)
        blob.upload_from_string(json.dumps(data, indent=2))
        
        logging.info(f"Cached Singapore API data to {cache_filename}")
        return cache_filename
    except Exception as e:
        logging.error(f"Failed to save cache: {e}")
        return None


@app.get("/positions")
async def get_data():
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(POSITION_FILE_NAME)

        if not blob.exists():
            raise HTTPException(status_code=404, detail="Data file not found.")

        content = blob.download_as_text()
        lines = content.strip().splitlines()

        data = {}
        for line in lines:
            if ':' in line:
                key, value = line.strip().split(':', 1)
                key = key.strip().lower()
                value = value.strip()
                if key in ["latitude", "longitude"]:
                    data[key] = float(value)
                else:
                    data[key] = value

        return data
    except Exception as e:
        logging.exception("Failed to read from GCS")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/positions")
async def submit_data(data: InputData):
    try:
        content = (
            f"Time: {data.time}\n"
            f"Latitude: {data.lat}\n"
            f"Longitude: {data.lon}\n"
        )
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(POSITION_FILE_NAME)
        blob.upload_from_string(content)
        return {"message": f"Data written to gs://{BUCKET_NAME}/{POSITION_FILE_NAME}"}
    except Exception as e:
        logging.exception("Failed to write to GCS")
        raise HTTPException(status_code=500, detail=str(e))
    
    
@app.get("/fuel-consumption")
async def get_fuel_consumption_data():
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(FUEL_CONSUMPTION_FILE_NAME)

        if not blob.exists():
            raise HTTPException(status_code=404, detail="Data file not found.")

        content = blob.download_as_text()
        data = json.loads(content)

        return data
    except Exception as e:
        logging.exception("Failed to read from GCS")
        raise HTTPException(status_code=500, detail=str(e))
    

class FuelRequest(BaseModel):
    category: str
    vessel_type: str
    speed: float  # knots

    
@app.post("/fuel-consumption")
async def get_fuel_consumption_value(request: FuelRequest):
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(FUEL_CONSUMPTION_FILE_NAME)

        if not blob.exists():
            raise HTTPException(status_code=404, detail="Data file not found.")

        content = blob.download_as_text()
        data = json.loads(content)

        category = request.category
        vessel_type = request.vessel_type
        speed = round(float(request.speed))

        # Clamp speed to available range
        clamped_speed = max(8, min(22, speed))

        vessel_info = data.get(category, {}).get(vessel_type)
        if not vessel_info:
            raise HTTPException(status_code=404, detail="Vessel type not found.")

        fuel_map = vessel_info.get("fuel_consumption_tpd", {})
        fuel_value = fuel_map.get(str(clamped_speed))

        if fuel_value is None:
            raise HTTPException(status_code=404, detail="Fuel data not available for this speed.")

        return {
            "category": category,
            "vessel_type": vessel_type,
            "requested_speed": request.speed,
            "used_speed": clamped_speed,
            "fuel_consumption_tpd": fuel_value
        }

    except Exception as e:
        logging.exception("Failed to compute fuel consumption")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ship", response_model=List[SingaporeApiResponse])
async def get_singapore_ship_data(x_api_key: str = Header(..., alias="X-API-Key")):
    """
    Endpoint to fetch ship data from Singapore API with caching.
    Checks for cached data within 10 minutes, otherwise fetches fresh data.
    
    Requires:
        - X-API-Key header for authentication with the Singapore API.
    """
    try:
        # Validate API key is provided
        if not x_api_key or x_api_key.strip() == "":
            raise HTTPException(status_code=400, detail="API key is required")
        
        # Check for cached data first
        cached_blob, cache_timestamp = get_latest_cache_file()
        
        if cached_blob is not None:
            logging.info(f"Using cached Singapore API data from {cache_timestamp}")
            cached_content = cached_blob.download_as_text()
            cached_data = json.loads(cached_content)
            return cached_data
        
        # No valid cache found, fetch from API
        logging.info("No valid cache found, fetching fresh data from Singapore API")
        
        headers = {
            'Content-Type': 'application/json',
            'apikey': x_api_key,
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                SINGAPORE_API_URL,
                headers=headers,
                timeout=30.0
            )
            
            if not response.is_success:
                logging.error(f"Singapore API request failed: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=502, 
                    detail=f"Failed to fetch data from Singapore API: {response.status_code}"
                )
            
            # Parse the response data
            api_data = response.json()
            
            # Transform the data to match our response model if needed
            # You may need to adjust this based on the actual API response structure
            transformed_data = []
            for ship in api_data:
                transformed_ship = {
                    "vesselParticulars": {
                        "vesselName": ship.get("vesselName", ""),
                        "callSign": ship.get("callSign", ""),
                        "imoNumber": ship.get("imoNumber", ""),
                        "flag": ship.get("flag", ""),
                        "vesselLength": float(ship.get("vesselLength", 0)),
                        "vesselBreadth": float(ship.get("vesselBreadth", 0)),
                        "vesselDepth": float(ship.get("vesselDepth", 0)),
                        "vesselType": ship.get("vesselType", ""),
                        "grossTonnage": float(ship.get("grossTonnage", 0)),
                        "netTonnage": float(ship.get("netTonnage", 0)),
                        "deadweight": float(ship.get("deadweight", 0)),
                        "mmsiNumber": ship.get("mmsiNumber", ""),
                        "yearBuilt": ship.get("yearBuilt", "")
                    },
                    "latitude": float(ship.get("latitude", 0)),
                    "longitude": float(ship.get("longitude", 0)),
                    "latitudeDegrees": float(ship.get("latitudeDegrees", ship.get("latitude", 0))),
                    "longitudeDegrees": float(ship.get("longitudeDegrees", ship.get("longitude", 0))),
                    "speed": float(ship.get("speed", 0)),
                    "course": float(ship.get("course", 0)),
                    "heading": float(ship.get("heading", 0)),
                    "timeStamp": ship.get("timeStamp", "")
                }
                transformed_data.append(transformed_ship)
            
            # Save the fetched data to cache
            save_to_cache(transformed_data)
            
            logging.info("Fetched and cached fresh Singapore API data")
            return transformed_data
        
    except httpx.RequestError as e:
        logging.exception("Network error when calling Singapore API")
        raise HTTPException(status_code=502, detail=f"Network error: {str(e)}")
    except httpx.HTTPStatusError as e:
        logging.exception("HTTP error from Singapore API")
        raise HTTPException(status_code=502, detail=f"API error: {e.response.status_code}")
    except Exception as e:
        logging.exception("Failed to fetch Singapore ship data")
        raise HTTPException(status_code=500, detail=str(e))