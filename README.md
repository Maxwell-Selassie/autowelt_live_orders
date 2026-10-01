# Autowelt Live Order Simulator

A deliberately small FastAPI source system for the Autowelt data engineering project.

The application simulates `ORDER_CREATED` business events and publishes them to Amazon Kinesis Data Streams when Kinesis is enabled.

## Architecture

```text
HTML/CSS
   |
   v
FastAPI
   |
   v
Order event
   |
   v
Amazon Kinesis Data Streams
   |
   v
Future streaming consumer
```

## Run locally without AWS

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy the environment file:

```bash
cp .env.example .env
```

Leave:

```text
KINESIS_ENABLED=false
```

Then start:

```bash
uvicorn app.main:app --reload
```

Open:

```text
http://127.0.0.1:8000
```

The UI will generate order events locally.

## Enable Kinesis

Create an AWS Kinesis Data Stream named:

```text
autowelt-orders
```

Set:

```text
KINESIS_ENABLED=true
AWS_REGION=eu-central-1
KINESIS_STREAM_NAME=autowelt-orders
```

The AWS credentials should come from the normal AWS credential chain. Do not hard-code credentials in the application.

The IAM identity running the application needs permission to put records into the stream, such as:

```text
kinesis:PutRecord
```

For the first version, the API uses the customer's ID as the Kinesis partition key. This keeps events from the same customer ordered within their Kinesis shard.

## API

### Health

```http
GET /health
```

### Simulate an order

```http
POST /api/v1/orders/simulate
Content-Type: application/json
```

Example:

```json
{
  "customer_id": "CUS-1001",
  "vehicle_id": "VEH-5001",
  "branch_id": "BR-DE-BER-01",
  "currency": "EUR",
  "amount": 68500,
  "quantity": 1
}
```

The response contains the generated business event and the Kinesis publication result.

## Important project boundary

This service intentionally does NOT implement:

- customer authentication
- payments
- inventory management
- deliveries
- PostgreSQL
- a full dealership application
- Spark
- Databricks
- Airflow
- Kafka
- the analytical data platform

It is a small operational source that generates realistic live order events.

Those events will later feed the streaming side of the Autowelt Data Engineering platform.
