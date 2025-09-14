from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, HttpUrl
import httpx
import os
from typing import Dict, Any
import logging
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError, BotoCoreError

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Dromo Headless Import API", version="1.0.0")

# Configuration
DROMO_API_BASE = "https://app.dromo.io/api/v1"
DROMO_LICENSE_KEY = os.getenv("DROMO_LICENSE_KEY")
DROMO_SCHEMA_ID = os.getenv("DROMO_SCHEMA_ID")

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

if not DROMO_LICENSE_KEY:
    logger.warning("DROMO_LICENSE_KEY environment variable not set")

if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_S3_BUCKET]):
    logger.warning("AWS credentials or S3 bucket not configured")

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)


class FileImportRequest(BaseModel):
    s3_key: str
    filename: str


class S3FileImportRequest(BaseModel):
    s3_key: str
    filename: str


class WebhookResponse(BaseModel):
    import_id: str
    status: str
    data: Dict[str, Any] = None
    errors: list = None



@app.get("/")
async def root():
    """
    Root endpoint returning hello world
    """
    return {"message": "Hello World"}


@app.post("/import-file")
async def import_file(
    request: S3FileImportRequest, background_tasks: BackgroundTasks
):
    """
    Trigger a headless import from an S3 file
    """
    if not DROMO_LICENSE_KEY:
        raise HTTPException(
            status_code=500, detail="Dromo license key not configured"
        )

    if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_S3_BUCKET]):
        raise HTTPException(
            status_code=500, detail="AWS credentials or S3 bucket not configured"
        )

    try:
        # Step 1: Read file from S3
        try:
            response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=request.s3_key)
            file_content = response['Body'].read()
            logger.info(f"Successfully read file {request.s3_key} from S3 bucket {AWS_S3_BUCKET}")
        except NoCredentialsError:
            raise HTTPException(status_code=500, detail="AWS credentials not found")
        except BotoCoreError as e:
            raise HTTPException(status_code=500, detail=f"Error reading from S3: {str(e)}")

        # Step 2: Create headless import
        headers = {
            "X-Dromo-License-Key": DROMO_LICENSE_KEY,
            "Content-Type": "application/json"
        }

        create_payload = {
            "schema_id": DROMO_SCHEMA_ID,
            "original_filename": request.filename
        }

        async with httpx.AsyncClient() as client:
            # Create import
            response = await client.post(
                f"{DROMO_API_BASE}/headless/imports/",
                json=create_payload,
                headers=headers
            )
            response.raise_for_status()
            import_data = response.json()
            
            logger.info(f"Dromo API response: {import_data}")

            if "id" not in import_data:
                raise ValueError(f"Missing 'id' in Dromo response: {import_data}")
                
            import_id = import_data["id"]
            
            # The upload URL might be under different keys
            upload_url = None
            for possible_key in ["upload_url", "upload", "uploadUrl", "file_upload_url"]:
                if possible_key in import_data:
                    upload_url = import_data[possible_key]
                    break
                    
            if not upload_url:
                raise ValueError(f"No upload URL found in Dromo response. Available keys: {list(import_data.keys())}")

            # Step 3: Upload file to Dromo
            upload_response = await client.put(
                upload_url, content=file_content
            )
            upload_response.raise_for_status()

            logger.info(
                f"Started import {import_id} for S3 file {request.s3_key}"
            )

            return {
                "import_id": import_id,
                "status": "PENDING",
                "message": "Import started successfully"
            }

    except httpx.HTTPStatusError as e:
        logger.error(
            f"HTTP error: {e.response.status_code} - {e.response.text}"
        )
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Dromo API error: {e.response.text}",
        )
    except Exception as e:
        logger.error(f"Error starting import: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to start import: {str(e)}"
        )


async def download_and_save_processed_data(import_id: str):
    """
    Download processed data from Dromo using presigned URL and save to S3
    """
    try:
        headers = {
            "X-Dromo-License-Key": DROMO_LICENSE_KEY,
        }

        # Get presigned download URL from Dromo
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                f"{DROMO_API_BASE}/upload/{import_id}/url/",
                headers=headers
            )
            response.raise_for_status()
            presigned_data = response.json()

            download_url = presigned_data.get("presigned_url")
            if not download_url:
                raise ValueError(f"No presigned_url in response: {presigned_data}")

            # Download the processed data
            download_response = await client.get(download_url)
            download_response.raise_for_status()
            processed_data = download_response.content

            # Create output filename
            output_key = f"processed/{import_id}.csv"

            # Write to S3
            s3_client.put_object(
                Bucket=AWS_S3_BUCKET,
                Key=output_key,
                Body=processed_data,
                ContentType='text/csv'
            )

            logger.info(f"Successfully downloaded and wrote processed data to S3: {output_key}")
            return output_key

    except Exception as e:
        logger.error(f"Error downloading and saving processed data: {str(e)}")
        raise


@app.post("/webhook")
async def webhook_handler(webhook_data: Dict[str, Any]):
    """
    Handle webhook notifications from Dromo and download processed data to S3
    """
    try:
        # Extract data from the correct webhook structure
        data = webhook_data.get("data", {})
        import_id = data.get("id")
        status = data.get("status")

        if not import_id:
            raise HTTPException(
                status_code=400, detail="Missing import_id in webhook data.data.id"
            )

        if status == "SUCCESSFUL":
            logger.info(f"Import {import_id} completed successfully")

            # Download processed data from Dromo and save to S3
            try:
                output_key = await download_and_save_processed_data(import_id)
                logger.info(f"Successfully processed import {import_id} - data saved to S3: {output_key}")
            except Exception as download_error:
                logger.error(f"Failed to download processed data for import {import_id}: {str(download_error)}")
                
        elif status == "FAILED":
            logger.error(f"Import {import_id} failed: {data.get('errors')}")

        return {"message": "Webhook processed successfully", "import_id": import_id, "status": status}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to process webhook: {str(e)}"
        )



@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
