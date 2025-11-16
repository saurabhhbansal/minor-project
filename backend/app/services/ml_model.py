import joblib
import os

MODEL_PATH = os.path.join(os.path.dirname(__file__), "../ml/rf_capital_model.pkl")
FEATURES_PATH = os.path.join(os.path.dirname(__file__), "../ml/features.pkl")
def load_model():
    model = joblib.load(MODEL_PATH)
    features = joblib.load(FEATURES_PATH)
    return model, features

def get_feature_importance():
    model, features = load_model()
    importances = model.feature_importances_

    result = [
        {"feature": f, "importance": float(i)}
        for f, i in zip(features, importances)
    ]

    result = sorted(result, key=lambda x: x["importance"], reverse=True)
    return result

def predict_capital_expenditure(library, new_equipment, workshop, studio, other):
    model, features = load_model()
    X = [[library, new_equipment, workshop, studio, other]]
    prediction = model.predict(X)[0]
    return float(prediction)
