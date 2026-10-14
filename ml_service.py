import os
import numpy as np
import pandas as pd
import joblib
from fastapi import FastAPI
from pydantic import BaseModel
from sklearn.ensemble import GradientBoostingRegressor

MODEL_DIR = "models"
os.makedirs(MODEL_DIR, exist_ok=True)

SERVICE_TYPES = ["general", "billing", "consultation"]

BASE = {
    "general":      {"mean": 4.0,  "sigma": 0.55},
    "billing":      {"mean": 6.5,  "sigma": 0.45},
    "consultation": {"mean": 12.0, "sigma": 0.70},
}

def hour_mult(h):
    if 10 <= h <= 12: return 1.30
    if 14 <= h <= 16: return 1.25
    if h < 8 or h > 20: return 0.70
    return 1.00

def dow_mult(d):
    return {0: 1.20, 1: 1.00, 2: 1.00, 3: 1.00, 4: 1.10, 5: 0.80, 6: 0.50}[d]

def generate(n=8000, seed=42):
    rng = np.random.default_rng(seed)
    rows = []
    for _ in range(n):
        st = rng.choice(SERVICE_TYPES)
        h = int(rng.integers(8, 20))
        d = int(rng.integers(0, 7))
        base = BASE[st]["mean"] * hour_mult(h) * dow_mult(d)
        dur = float(np.exp(rng.normal(np.log(base), BASE[st]["sigma"])))
        rows.append({"serviceType": st, "hour": h, "dayOfWeek": d, "durationMin": dur})
    return pd.DataFrame(rows)

def featurize(df):
    X = pd.DataFrame(index=df.index)
    for st in SERVICE_TYPES:
        X[f"st_{st}"] = (df["serviceType"] == st).astype(int)
    X["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    X["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    X["dow_sin"]  = np.sin(2 * np.pi * df["dayOfWeek"] / 7)
    X["dow_cos"]  = np.cos(2 * np.pi * df["dayOfWeek"] / 7)
    return X

MODELS = {}

def train_all():
    df = generate(8000)
    X = featurize(df)
    y = df["durationMin"].values
    for alpha, name in [(0.1, "p10"), (0.5, "p50"), (0.9, "p90")]:
        m = GradientBoostingRegressor(
            loss="quantile", alpha=alpha,
            n_estimators=180, max_depth=4, learning_rate=0.08,
        )
        m.fit(X, y)
        MODELS[name] = m
        joblib.dump(m, f"{MODEL_DIR}/{name}.pkl")
    print(f"[ml] trained p10/p50/p90 on {len(df)} samples")

def load_or_train():
    try:
        for name in ("p10", "p50", "p90"):
            MODELS[name] = joblib.load(f"{MODEL_DIR}/{name}.pkl")
        print("[ml] loaded saved models")
    except Exception:
        train_all()

app = FastAPI()

class QueueItem(BaseModel):
    serviceType: str
    state: str
    elapsedMs: float = 0

class PredictRequest(BaseModel):
    hour: int
    dayOfWeek: int
    ahead: list[QueueItem]

class DurationStatsRequest(BaseModel):
    serviceTypes: list[str]
    hour: int
    dayOfWeek: int

@app.on_event("startup")
def _startup():
    load_or_train()

@app.get("/health")
def health():
    return {"ok": True, "models": list(MODELS.keys())}

@app.post("/retrain")
def retrain():
    train_all()
    return {"ok": True}

@app.post("/duration_stats")
def duration_stats(req: DurationStatsRequest):
    """Return p10/p50/p90 (minutes) for each requested service type at given time."""
    out = {}
    for st in req.serviceTypes:
        if st not in SERVICE_TYPES:
            # fallback
            out[st] = {"p10": 4.0, "p50": 5.0, "p90": 7.0}
            continue
        row = pd.DataFrame([{"serviceType": st, "hour": req.hour, "dayOfWeek": req.dayOfWeek}])
        X = featurize(row)
        p10 = float(MODELS["p10"].predict(X)[0])
        p50 = float(MODELS["p50"].predict(X)[0])
        p90 = float(MODELS["p90"].predict(X)[0])
        out[st] = {"p10": p10, "p50": p50, "p90": p90}
    return out

@app.post("/predict")
def predict(req: PredictRequest):
    """Legacy single-queue prediction (kept for compatibility)."""
    if not req.ahead:
        return {"etaMinutes": 0.0, "p10Minutes": 0.0, "p90Minutes": 0.0}

    n_sims = 400
    sims = np.zeros(n_sims)
    for item in req.ahead:
        row = pd.DataFrame([{
            "serviceType": item.serviceType,
            "hour": req.hour,
            "dayOfWeek": req.dayOfWeek,
        }])
        X = featurize(row)
        p10 = float(MODELS["p10"].predict(X)[0])
        p50 = float(MODELS["p50"].predict(X)[0])
        p90 = float(MODELS["p90"].predict(X)[0])
        sigma = max(0.5, (p90 - p10) / (2 * 1.2816))
        samples = np.random.normal(p50, sigma, n_sims).clip(0.2, None)
        if item.state == "serving":
            samples = np.maximum(0, samples - item.elapsedMs / 60000.0)
        sims += samples

    return {
        "etaMinutes": float(np.median(sims)),
        "p10Minutes": float(np.quantile(sims, 0.10)),
        "p90Minutes": float(np.quantile(sims, 0.90)),
    }