# backend/app/services/storage_utils.py
from supabase import create_client
from supabase import StorageException
import os
from typing import Optional
from dotenv import load_dotenv
import json
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def upload_file(bucket: str, local_path: str, storage_path: Optional[str] = None, upsert: str = "true"):
    storage_path = storage_path or os.path.basename(local_path)

    print("\n=============== STORAGE UPLOAD DEBUG ===============")
    print("Bucket:", bucket)
    print("Local path:", local_path)
    print("Storage path:", storage_path)
    print("----------------------------------------------------")
    print("File options being passed:")
    print(json.dumps({
        "cacheControl": "3600",
        "contentType": "text/csv",
        "upsert": upsert
    }, indent=4))
    print("====================================================\n")

    with open(local_path, "rb") as f:
        # 🔥 ADD DEBUG LOGS OF FINAL REQUEST HEADERS
        try:
            result = sb.storage.from_(bucket).upload(
                path=storage_path,
                file=f,
                file_options={
                    "cacheControl": "3600",
                    "contentType": "text/csv",
                    "upsert":upsert
                }
            )
            print("UPLOAD RESULT:")
            print(result)
            return result

        except Exception as e:
            print("\n🔥🔥 UPLOAD FAILED — RAW ERROR BELOW 🔥🔥")
            print(e)
            print("----------------------------------------------------")
            raise

def download_file(bucket: str, storage_path: str, local_dest: str):
    data = sb.storage.from_(bucket).download(storage_path)
    with open(local_dest, "wb") as f:
        f.write(data)
    return local_dest

def generate_signed_url(bucket: str, storage_path: str, expires_in: int = 3600):
    r = sb.storage.from_(bucket).create_signed_url(storage_path, expires_in)
    return r

def list_files(bucket: str, prefix: Optional[str] = None):
    return sb.storage.from_(bucket).list(prefix=prefix)
def file_exists(bucket: str, path: str) -> bool:
    """Check if a file exists in a Supabase bucket."""
    try:
        files = sb.storage.from_(bucket).list()
        return any(f["name"] == path for f in files)
    except Exception:
        return False