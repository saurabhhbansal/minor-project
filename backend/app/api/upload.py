from fastapi import APIRouter, UploadFile, File, Form
from typing import List, Optional
from scripts.process_batch import process_batch
import tempfile
import shutil

router = APIRouter()

def save_upload_temp(upload: UploadFile) -> str:
    suffix = upload.filename.split(".")[-1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{suffix}")

    with tmp as buffer:
        shutil.copyfileobj(upload.file, buffer)

    return tmp.name

@router.post("/upload/pdf")   # <---- FIXED
async def upload_pdf(
    college_type: str = Form(...),

    # OPTIONAL single PDF
    file: Optional[UploadFile] = File(None),

    # OPTIONAL folder (multiple PDFs)
    folder_files: Optional[List[UploadFile]] = File(None),
):
    if not file and not folder_files:
        return {"error": "No PDF(s) uploaded"}

    results = []

    # SINGLE PDF
    if file:
        original_name = file.filename              # <-- keep actual file name
        temp_path = save_upload_temp(file)
        
        res = process_batch(
            input_path=temp_path,
            college_type=college_type,
            original_filename=original_name        # <-- pass real name
        )

        results.append({"filename": original_name, "result": res})

    # ------------ FOLDER MODE ------------
    if folder_files:
        for pdf in folder_files:
            if not pdf.filename.lower().endswith(".pdf"):
                continue

            original_name = pdf.filename
            temp_path = save_upload_temp(pdf)

            res = process_batch(
                input_path=temp_path,
                college_type=college_type,
                original_filename=original_name
            )

            results.append({"filename": original_name, "result": res})

    return {"status": "ok", "results": results}
