import json
from datetime import datetime
from typing import List, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, insert, select, update, delete
from pydantic import BaseModel, field_validator

from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)


# --- Pydantic Models ---

class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator('timestamp', mode='before')
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError("Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).")


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# --- FastAPI App ---

app = FastAPI()

# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()


@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)


async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(json.dumps(data, default=str))


# --- CRUD Endpoints ---

@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    with engine.connect() as conn:
        rows = []
        for item in data:
            row = {
                "road_state": item.road_state,
                "x": item.agent_data.accelerometer.x,
                "y": item.agent_data.accelerometer.y,
                "z": item.agent_data.accelerometer.z,
                "latitude": item.agent_data.gps.latitude,
                "longitude": item.agent_data.gps.longitude,
                "timestamp": item.agent_data.timestamp,
            }
            rows.append(row)

        conn.execute(insert(processed_agent_data), rows)
        conn.commit()

    await send_data_to_subscribers([r for r in rows])
    return {"status": "ok", "inserted": len(rows)}


@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int):
    with engine.connect() as conn:
        result = conn.execute(
            select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        ).fetchone()

    if result is None:
        raise HTTPException(status_code=404, detail="Record not found")

    return ProcessedAgentDataInDB(**result._mapping)


@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data():
    with engine.connect() as conn:
        results = conn.execute(select(processed_agent_data)).fetchall()

    return [ProcessedAgentDataInDB(**row._mapping) for row in results]


@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    with engine.connect() as conn:
        existing = conn.execute(
            select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        ).fetchone()

        if existing is None:
            raise HTTPException(status_code=404, detail="Record not found")

        conn.execute(
            update(processed_agent_data)
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .values(
                road_state=data.road_state,
                x=data.agent_data.accelerometer.x,
                y=data.agent_data.accelerometer.y,
                z=data.agent_data.accelerometer.z,
                latitude=data.agent_data.gps.latitude,
                longitude=data.agent_data.gps.longitude,
                timestamp=data.agent_data.timestamp,
            )
        )
        conn.commit()

        updated = conn.execute(
            select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        ).fetchone()

    return ProcessedAgentDataInDB(**updated._mapping)


@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int):
    with engine.connect() as conn:
        existing = conn.execute(
            select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        ).fetchone()

        if existing is None:
            raise HTTPException(status_code=404, detail="Record not found")

        conn.execute(
            delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        )
        conn.commit()

    return ProcessedAgentDataInDB(**existing._mapping)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
