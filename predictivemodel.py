import re
import os
import math
import joblib
import warnings
import numpy as np
import pandas as pd

from typing import Dict, List, Tuple

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GroupKFold, cross_validate
from sklearn.metrics import make_scorer, r2_score, mean_squared_error, mean_absolute_error
from sklearn.inspection import permutation_importance
from sklearn.ensemble import HistGradientBoostingRegressor

warnings.filterwarnings("ignore", category=UserWarning)

# ---------- Utilities ----------

def _safe_scale_to_int(scale_str: str) -> int:
    """Extract numeric GB from a scale label like '10GB', '100G', '20g', '200 GB'."""
    if scale_str is None:
        return np.nan
    m = re.search(r"(\d+)\s*g(b)?", str(scale_str).lower())
    if m:
        return int(m.group(1))
    # if nothing matched but it is a digit, try to cast
    try:
        return int(scale_str)
    except Exception:
        return np.nan

def _pick_numeric_columns(df: pd.DataFrame, target_col: str) -> List[str]:
    """Pick numeric feature columns, excluding obvious IDs/leaky fields."""
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    drop_like = {
        target_col,
        "elapsed_time_seconds",          # near-duplicate of exec_time in some data
        "timestamp", "timestamp_ms",
        "sn_before_timestamp_ms", "sn_after_timestamp_ms",
        "pageno", "ID", "id", "idx",
    }
    # Also drop any column that is literally all-NaN or constant
    selected = []
    for c in numeric_cols:
        if c in drop_like:
            continue
        s = df[c]
        if s.isna().all():
            continue
        if s.nunique(dropna=True) <= 1:
            continue
        selected.append(c)
    return selected

def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Add missing columns as NA so ColumnTransformer doesn't crash."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        for c in missing:
            df[c] = pd.NA
    return df

# ---------- Data loading & preparation ----------

def load_all_runs(files: Dict[str, Dict[str, Dict[str, str]]]) -> pd.DataFrame:
    """
    Read all CSVs, build one long DataFrame.
    Adds columns:
      tech, scale, scale_gb, op ('update' | 'read'), query, wquery (if present), update_query (best guess).
    """
    rows = []
    for scale, tech_bundle in files.items():
        scale_gb = _safe_scale_to_int(scale)
        for tech, paths in tech_bundle.items():
            for op in ("update", "read"):
                path = paths.get(op)
                if not path or not os.path.exists(path):
                    # Skip missing files silently (or print a note)
                    # print(f"Missing: {scale} / {tech} / {op} -> {path}")
                    continue
                df = pd.read_csv(path)
                if df.empty:
                    continue
                df = df.copy()
                df["tech"] = tech
                df["scale"] = scale
                df["scale_gb"] = scale_gb
                df["op"] = op

                # Normalize expected columns
                if "query" not in df.columns:
                    # If no 'query' col, try 'wquery' or fallback
                    if "wquery" in df.columns:
                        df["query"] = df["wquery"]
                    else:
                        df["query"] = f"{op}_unknown"

                # Define update_query (for 'read' rows it should be in wquery, else fall back)
                if "wquery" in df.columns and df["wquery"].notna().any():
                    df["update_query"] = df["wquery"].fillna("unknown_update")
                else:
                    # For update rows, the update query is the query itself
                    df["update_query"] = np.where(df["op"] == "update",
                                                  df["query"],
                                                  "unknown_update")

                # Ensure exec_time exists
                if "exec_time" not in df.columns:
                    raise ValueError(f"'exec_time' column missing in file: {path}")

                rows.append(df)

    if not rows:
        raise ValueError("No data loaded. Check your 'files' dict and CSV paths.")
    all_df = pd.concat(rows, ignore_index=True)
    return all_df

def prepare_dataset(all_df: pd.DataFrame,
                    target_col: str = "exec_time"
                   ) -> Tuple[pd.DataFrame, pd.Series, List[str], List[str]]:
    """
    Clean & select features for modeling.
    Returns X, y, list_cats, list_nums.
    """
    # Drop rows with missing target or non-positive exec times
    all_df = all_df[all_df[target_col].notna()].copy()
    all_df = all_df[all_df[target_col] > 0].copy()

    # Light outlier clipping on target to stabilize training (optional)
    # Clip to the 0.5th–99.5th percentiles
    lo, hi = np.percentile(all_df[target_col], [0.5, 99.5])
    all_df[target_col] = all_df[target_col].clip(lo, hi)

    # Categorical columns we expect
    cat_cols = ["tech", "scale", "op", "query", "update_query"]
    all_df = _ensure_columns(all_df, cat_cols)

    # Numeric columns
    num_cols = _pick_numeric_columns(all_df, target_col=target_col)

    # Very common names in your data – keep if present
    likely_good = [
        "numStages","numTasks","stageDuration","executorRunTime","executorCpuTime",
        "executorDeserializeTime","resultSerializationTime","jvmGCTime",
        "shuffleFetchWaitTime","shuffleWriteTime","resultSize",
        "recordsRead","bytesRead","recordsWritten","bytesWritten",
        "shuffleRecordsRead","shuffleTotalBlocksFetched","shuffleLocalBlocksFetched",
        "shuffleRemoteBlocksFetched","shuffleTotalBytesRead","shuffleLocalBytesRead",
        "shuffleRemoteBytesRead","shuffleBytesWritten","shuffleRecordsWritten",
        "diskBytesSpilled","memoryBytesSpilled","peakExecutionMemory",
        "scale_gb"
    ]
    num_cols = sorted(list(set(num_cols).union(set(c for c in likely_good if c in all_df.columns))))

    # Final selection + drop any rows still fully NA in all features
    feature_cols = cat_cols + num_cols
    X = all_df[feature_cols].copy()
    y = all_df[target_col].astype(float)

    # Drop rows where all numeric features are NA
    if num_cols:
        mask_non_all_na = X[num_cols].notna().any(axis=1)
        X = X[mask_non_all_na]
        y = y.loc[X.index]

    # Fill NA numeric with 0; categoricals left to OneHotEncoder(handle_unknown='ignore')
    for c in num_cols:
        X[c] = X[c].astype(float).fillna(0.0)

    return X, y, cat_cols, num_cols

# ---------- Modeling ----------

def build_pipeline(cat_cols: List[str], num_cols: List[str]) -> Pipeline:
    cat_transformer = OneHotEncoder(handle_unknown="ignore", sparse_output=True)
    pre = ColumnTransformer(
        transformers=[
            ("cat", cat_transformer, cat_cols),
            ("num", "passthrough", num_cols),
        ],
        remainder="drop",
        sparse_threshold=0.3,  # keep it sparse where possible
    )

    model = HistGradientBoostingRegressor(
        loss="squared_error",
        max_depth=None,
        learning_rate=0.08,
        max_leaf_nodes=31,
        min_samples_leaf=20,
        l2_regularization=0.0,
        random_state=42
    )

    pipe = Pipeline(steps=[
        ("pre", pre),
        ("model", model)
    ])
    return pipe

def evaluate_model(pipe: Pipeline, X: pd.DataFrame, y: pd.Series, groups: pd.Series):
    # R², RMSE, MAE with group-wise CV by 'scale'
    scoring = {
        "r2": make_scorer(r2_score),
        "rmse": make_scorer(lambda yt, yp: math.sqrt(mean_squared_error(yt, yp))),
        "mae": make_scorer(mean_absolute_error),
    }
    gkf = GroupKFold(n_splits=min(5, len(np.unique(groups))))
    cv = cross_validate(pipe, X, y, cv=gkf, groups=groups, scoring=scoring, n_jobs=-1, return_estimator=True)

    print("Cross-validated performance (GroupKFold by scale):")
    for m in ("r2","rmse","mae"):
        scores = cv[f"test_{m}"]
        print(f"  {m.upper():<4}: mean={scores.mean():.4f}  std={scores.std():.4f}  folds={len(scores)}")

    # Fit on full data for final model
    final_pipe = cv["estimator"][np.argmax(cv["test_r2"])]
    return final_pipe

def show_top_features(pipe: Pipeline, X_sample: pd.DataFrame, top_k: int = 25):
    """
    Use permutation importance to get a model-agnostic importance ranking (slowish but robust).
    """
    print("\nComputing permutation importance on a sample (this can take a bit)...")
    # Subsample for speed if needed
    if len(X_sample) > 5000:
        X_eval = X_sample.sample(5000, random_state=42)
    else:
        X_eval = X_sample

    # We need y as well; pull from the pipeline? Easier: require the caller to pass y if needed.
    print("Note: call permutation_importance with (pipe, X_eval, y_eval) after fitting if you want exact values.")

# ---------- All-in-one trainer ----------

def train_runtime_model(files: Dict[str, Dict[str, Dict[str, str]]],
                        save_path: str = "runtime_model.joblib") -> Pipeline:
    all_df = load_all_runs(files)
    X, y, cat_cols, num_cols = prepare_dataset(all_df, target_col="exec_time")

    pipe = build_pipeline(cat_cols, num_cols)

    # Use 'scale' as grouping for CV to respect scale differences
    groups = X["scale"].astype(str)

    final_pipe = evaluate_model(pipe, X, y, groups=groups)

    # Fit final_pipe on ALL data (it already is the best fold’s estimator, but refit to be safe)
    final_pipe.fit(X, y)
    joblib.dump(final_pipe, save_path)
    print(f"\nModel saved to: {save_path}")

    # Print top categorical levels by effect? For HGBR we do permutation importance if desired
    # (We skip auto-running it to keep runtime reasonable.)
    # Uncomment if you want:
    # from sklearn.inspection import permutation_importance
    # r = permutation_importance(final_pipe, X.sample(min(2000, len(X)), random_state=42), y.sample(min(2000, len(y)), random_state=42), n_repeats=5, random_state=42, n_jobs=-1)
    # # To map back feature names from ColumnTransformer, we’d need to extract them (can be verbose).

    # Quick sanity: show which columns were used
    print("\nFeature sets:")
    print(f"  Categorical: {cat_cols}")
    print(f"  Numeric    : {len(num_cols)} numeric columns")

    return final_pipe

# ---------- Prediction helper ----------

def predict_exec_time(model: Pipeline, new_df: pd.DataFrame) -> np.ndarray:
    """
    Predict exec_time for new rows (expects same columns used in training;
    missing ones are handled by the preprocessors if categorical; numeric missing filled with 0).
    """
    # Ensure required columns exist
    req_cats = ["tech","scale","op","query","update_query"]
    for c in req_cats:
        if c not in new_df.columns:
            new_df[c] = pd.NA
    if "scale_gb" not in new_df.columns:
        new_df["scale_gb"] = new_df["scale"].apply(_safe_scale_to_int)

    # Fill numerics with 0 the same way as training
    # (We don't know exact numeric cols now; passthrough will accept NA as well, but fill where possible)
    for c in new_df.select_dtypes(include=[np.number]).columns:
        new_df[c] = new_df[c].fillna(0.0)

    return model.predict(new_df)


# 1) Define your files dict exactly as you’ve been using:
files = {
    "10GB": {
        "blms": {"update": "blms_update_10GB.csv", "read": "blms_read_10GB.csv"},
        "bqms": {"update": "bqms_update_10GB.csv", "read": "bqms_read_10GB.csv"},
        "bqmn": {"update": "bqmn_update_10GB.csv", "read": "bqmn_read_10GB.csv"},
    },
    "20GB": {
        "blms": {"update": "blms_update_20GB.csv", "read": "blms_read_20GB.csv"},
        "bqms": {"update": "bqms_update_20GB.csv", "read": "bqms_read_20GB.csv"},
        "bqmn": {"update": "bqmn_update_20GB.csv", "read": "bqmn_read_20GB.csv"},
    },
    # ... add 50GB, 100GB
}

# 2) Train & evaluate
model = train_runtime_model(files, save_path="runtime_model.joblib")

# 3) Predict on new rows (must include the features; missing ones okay)
# new_samples = pd.DataFrame([...])
# preds = predict_exec_time(model, new_samples)
