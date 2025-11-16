from fastapi import APIRouter
from app.services.ml_model import get_feature_importance

router = APIRouter()

@router.get("/feature_imp")
def feature_importance():
    return {"feature_importance": get_feature_importance()}
