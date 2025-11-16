from fastapi import APIRouter
from app.services.storage_utils import sb
import logging

router = APIRouter()
LOG = logging.getLogger(__name__)

PDF_BUCKET = "pdfs"
CSV_BUCKET = "csvs"


def list_bucket_files(bucket: str):
    """Return list of files from a Supabase storage bucket."""
    try:
        files = sb.storage.from_(bucket).list()
        cleaned = [
            {
                "name": f["name"],
                "id": f["id"],
                "updated_at": f["updated_at"],
                "size": f["metadata"]["size"] if "metadata" in f else None,
                "url": sb.storage.from_(bucket).get_public_url(f["name"])
            }
            for f in files
        ]
        return cleaned

    except Exception as e:
        LOG.error(f"Error listing bucket {bucket}: {e}")
        return []


@router.get("/list_pdfs")
def list_pdfs():
    return {"bucket": PDF_BUCKET, "files": list_bucket_files(PDF_BUCKET)}


@router.get("/list_csvs")
def list_csvs():
    return {"bucket": CSV_BUCKET, "files": list_bucket_files(CSV_BUCKET)}


@router.get("/storage/list_all")
def list_all():
    return {
        "pdfs": list_bucket_files(PDF_BUCKET),
        "csvs": list_bucket_files(CSV_BUCKET)
    }
