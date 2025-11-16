from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from app.db import engine
from app.services.ml_model import predict_capital_expenditure

router = APIRouter()


class PredictRequest(BaseModel):
    institute_name: str
    year: str  # frontend may send "2022" or "2022-23"


def format_year_for_db(year_str: str):
    """
    Convert 2022 -> 2022-23 if needed.
    If already in academic format, return as-is.
    """
    if "-" in year_str:
        return year_str  # already correct

    y = int(year_str)
    return f"{y}-{str(y + 1)[-2:]}"


@router.post("/capital_predict")
def predict_by_institute(data: PredictRequest):

    institute = data.institute_name.strip()
    year = format_year_for_db(data.year.strip())

    # SQL using LONG format (category + amount)
    inst_pattern = f"%{institute}%"
    query = text("""
        SELECT category, amount
        FROM expenditures
        WHERE LOWER(institution_name) LIKE LOWER(:inst)
        AND year = :yr
        AND category IN (
                'Library',
                'New Equipment for Laboratories',
                'Engineering Workshop',
                'Studio',
                'Other'
        );
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"inst": inst_pattern, "yr": year}).fetchall()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No expenditure data found for '{institute}' in year '{year}'."
        )

    # default values for pivot
    data_wide = {
        "library": 0,
        "new_equipment": 0,
        "engineering_workshop": 0,
        "studio": 0,
        "other": 0
    }
    for(category,amount) in rows:
        print(category,amount)
    # pivot long → wide
    for category, amount in rows:
        c = category.lower()

        if c == "library":
            data_wide["library"] = amount

        elif c == "new equipment for laboratories":
            data_wide["new_equipment"] = amount

        elif c == "engineering workshop":
            data_wide["engineering_workshop"] = amount

        elif c == "studio":
            data_wide["studio"] = amount

        elif c == "other":
            data_wide["other"] = amount

    # order for ML model
    values = [
        data_wide["library"],
        data_wide["new_equipment"],
        data_wide["engineering_workshop"],
        data_wide["studio"],
        data_wide["other"],
    ]

    prediction = predict_capital_expenditure( 
        data_wide["library"],
        data_wide["new_equipment"],
        data_wide["engineering_workshop"],
        data_wide["studio"],
        data_wide["other"],
    )

    return {
        "institute": institute,
        "year": year,
        "input_values": data_wide,
        "predicted_total_capital_expenditure": prediction
    }
