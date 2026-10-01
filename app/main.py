from pathlib import Path
import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(
    title="Autowelt Live Order Simulator",
    version="1.0.0",
    description="Small FastAPI source system that publishes simulated vehicle orders to Amazon Kinesis Data Streams.",
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "autowelt-orders")

# Set KINESIS_ENABLED=false for local development without AWS.
KINESIS_ENABLED = os.getenv("KINESIS_ENABLED", "false").lower() == "true"

kinesis = (
    boto3.client("kinesis", region_name=AWS_REGION)
    if KINESIS_ENABLED
    else None
)


class OrderRequest(BaseModel):
    customer_id: str = Field(..., min_length=3)
    vehicle_id: str = Field(..., min_length=3)
    branch_id: str = Field(..., min_length=3)
    currency: str = Field(default="EUR", min_length=3, max_length=3)
    amount: Decimal = Field(..., gt=0)
    quantity: int = Field(default=1, ge=1, le=10)


def build_order(request: OrderRequest) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_version": 1,
        "order_id": f"ORD-{uuid.uuid4().hex[:10].upper()}",
        "customer_id": request.customer_id,
        "vehicle_id": request.vehicle_id,
        "branch_id": request.branch_id,
        "quantity": request.quantity,
        "currency": request.currency.upper(),
        "amount": float(request.amount),
        "order_status": "PLACED",
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def publish_to_kinesis(order: dict) -> dict:
    if not KINESIS_ENABLED or kinesis is None:
        return {
            "published": False,
            "mode": "local",
            "message": "Kinesis publishing is disabled. Event generated locally.",
        }

    payload = json.dumps(order).encode("utf-8")
    partition_key = order["customer_id"]

    try:
        response = kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=payload,
            PartitionKey=partition_key,
        )

        return {
            "published": True,
            "mode": "kinesis",
            "stream": KINESIS_STREAM_NAME,
            "shard_id": response["ShardId"],
            "sequence_number": response["SequenceNumber"],
        }

    except (BotoCoreError, ClientError) as exc:
        return {
            "published": False,
            "mode": "kinesis",
            "error": str(exc),
        }


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "kinesis_enabled": KINESIS_ENABLED,
        "stream": KINESIS_STREAM_NAME,
        "region": AWS_REGION,
    }


@app.post("/api/v1/orders/simulate")
async def simulate_order(order_request: OrderRequest):
    order = build_order(order_request)
    publish_result = publish_to_kinesis(order)

    status_code = 201 if publish_result.get("published", True) else 503

    return JSONResponse(
        status_code=status_code,
        content={
            "order": order,
            "delivery": publish_result,
        },
    )
