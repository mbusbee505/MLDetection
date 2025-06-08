from __future__ import annotations
import pathlib, joblib, pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from mldetection.features import NUMERIC, CATEGORICAL

def _pre() -> ColumnTransformer:
    return ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
        ("num", "passthrough", NUMERIC),
    ])

def train_iso(df: pd.DataFrame, *, contamination: float = 0.01, n_estimators: int = 300, random_state: int = 42) -> Pipeline:
    pipe = Pipeline([
        ("pre", _pre()),
        ("model", IsolationForest(contamination=contamination, n_estimators=n_estimators, random_state=random_state)),
    ])
    pipe.fit(df[NUMERIC + CATEGORICAL])
    return pipe

def save(pipe: Pipeline, path: pathlib.Path | str):
    joblib.dump(pipe, path)

def load(path: pathlib.Path | str) -> Pipeline:
    return joblib.load(path)
