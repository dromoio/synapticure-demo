# Dromo Headless Import FastAPI

A FastAPI application that provides endpoints for triggering Dromo headless imports from file URLs and handling webhook responses.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export DROMO_LICENSE_KEY=your_dromo_license_key_here
```

3. Run the application:
```bash
python main.py
```

The API will be available at `http://localhost:8000` (local) or `https://synapticure-demo.onrender.com` (production)

## Endpoints

### POST /import-file
Triggers a headless import from an S3 key.

**Request Body:**
```json
{
  "s3_key": "uploads/dromo_poc.csv",
  "filename": "dromo_poc.csv"
}
```

**Response:**
```json
{
  "import_id": "import_123",
  "status": "PENDING",
  "message": "Import started successfully"
}
```

**Example curl commands:**

Localhost:
```bash
curl -X POST "http://localhost:8000/import-file" \
  -H "Content-Type: application/json" \
  -d '{
    "s3_key": "uploads/dromo_poc.csv",
    "filename": "dromo_poc.csv"
  }'
```

Production:
```bash
curl -X POST "https://synapticure-demo.onrender.com/import-file" \
  -H "Content-Type: application/json" \
  -d '{
    "s3_key": "dromo_poc.csv",
    "filename": "dromo_poc.csv"
  }'
```

### POST /webhook
Handles webhook notifications from Dromo when imports complete.

### GET /import-status/{import_id}
Get the status of a specific import.

### GET /imports
List all imports and their statuses.

### GET /health
Health check endpoint.

## Webhook Configuration

Configure your Dromo dashboard to send webhooks to:
- Local: `http://localhost:8000/webhook`
- Production: `https://synapticure-demo.onrender.com/webhook`

## Documentation

Interactive API documentation is available at:
- Local Swagger UI: `http://localhost:8000/docs`
- Local ReDoc: `http://localhost:8000/redoc`
- Production Swagger UI: `https://synapticure-demo.onrender.com/docs`
- Production ReDoc: `https://synapticure-demo.onrender.com/redoc`