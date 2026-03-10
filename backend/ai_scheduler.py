import os
from datetime import datetime
from typing import Dict, Iterable, List, Optional

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None

try:
    from sklearn.ensemble import RandomForestRegressor
except Exception:  # pragma: no cover
    RandomForestRegressor = None


MODEL = None
MODEL_READY = False


def _as_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        return datetime.now()
    text = str(value).strip()
    if not text:
        return datetime.now()
    text = text.replace("T", " ")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.now()


def _processing_minutes(job: Dict) -> float:
    if "processing_time" in job and job.get("processing_time") is not None:
        return float(job["processing_time"])
    return float(job.get("duration_minutes") or 0)


def _hours_until_due(due_date, now: Optional[datetime] = None) -> float:
    ref = now or datetime.now()
    due = _as_datetime(due_date)
    return (due - ref).total_seconds() / 3600.0


def _machine_is_available(machine: Dict, mode: str) -> bool:
    status = str(machine.get("status") or "")
    if mode == "legacy":
        return status.lower() in ("available", "busy")
    return status in ("Available", "Busy")


def _worker_is_eligible(worker: Dict, mode: str) -> bool:
    if mode == "legacy":
        return int(worker.get("current_attendance") or 0) == 1 and str(worker.get("status") or "").lower() == "active"
    return str(worker.get("leave_status") or "") == "Present"


def _skill_text(worker: Dict, mode: str) -> str:
    if mode == "legacy":
        return str(worker.get("skill_category") or "").lower()
    return str(worker.get("skills") or "").lower()


def _build_job_features(
    job: Dict,
    machines: Optional[Iterable[Dict]] = None,
    workers: Optional[Iterable[Dict]] = None,
    now: Optional[datetime] = None,
    mode: str = "admin",
) -> List[float]:
    required_machine = str(job.get("required_machine_purpose") or "").strip().lower()
    required_skill = str(job.get("required_skill") or "").strip().lower()

    machine_available = 0
    if machines:
        for machine in machines:
            machine_purpose = str(machine.get("purpose") or "").strip().lower()
            if machine_purpose == required_machine and _machine_is_available(machine, mode):
                machine_available = 1
                break

    worker_skill_match = 0
    if workers:
        for worker in workers:
            if not _worker_is_eligible(worker, mode):
                continue
            if required_skill and required_skill in _skill_text(worker, mode):
                worker_skill_match = 1
                break

    return [
        float(_processing_minutes(job)),
        float(job.get("priority") or 0),
        float(_hours_until_due(job.get("due_date"), now=now)),
        float(machine_available),
        float(worker_skill_match),
    ]


def train_model(training_csv_path: str = "training_jobs.csv") -> bool:
    global MODEL, MODEL_READY
    MODEL_READY = False

    if pd is None or RandomForestRegressor is None:
        MODEL = None
        return False

    if not os.path.isabs(training_csv_path):
        training_csv_path = os.path.join(os.path.dirname(__file__), training_csv_path)

    if not os.path.exists(training_csv_path):
        MODEL = None
        return False

    data = pd.read_csv(training_csv_path)
    required_cols = {
        "processing_time",
        "priority",
        "hours_until_due",
        "machine_match",
        "skill_match",
        "delay",
    }
    if not required_cols.issubset(set(data.columns)):
        MODEL = None
        return False

    x = data[["processing_time", "priority", "hours_until_due", "machine_match", "skill_match"]]
    y = data["delay"]
    model = RandomForestRegressor(n_estimators=200, random_state=42)
    model.fit(x, y)
    MODEL = model
    MODEL_READY = True
    return True


def predict_job_score(features: List[float]) -> float:
    if MODEL_READY and MODEL is not None:
        return float(MODEL.predict([features])[0])

    # Deterministic fallback to preserve current behavior when AI assets are unavailable.
    processing_time, priority, hours_until_due, machine_available, worker_skill_match = features
    urgency_penalty = max(0.0, 24.0 - hours_until_due)
    resource_penalty = 20.0 * (1.0 - machine_available) + 20.0 * (1.0 - worker_skill_match)
    return (processing_time * 0.1) - (priority * 2.0) + urgency_penalty + resource_penalty


def rank_jobs(
    jobs: List[Dict],
    machines: Optional[Iterable[Dict]] = None,
    workers: Optional[Iterable[Dict]] = None,
    now: Optional[datetime] = None,
    mode: str = "admin",
) -> List[Dict]:
    if not jobs:
        return []

    if not MODEL_READY:
        train_model()

    ranked = []
    ref_now = now or datetime.now()
    for job in jobs:
        features = _build_job_features(job, machines=machines, workers=workers, now=ref_now, mode=mode)
        score = predict_job_score(features)
        ranked.append((score, job))

    ranked.sort(
        key=lambda item: (
            item[0],
            -float(item[1].get("priority") or 0),
            _as_datetime(item[1].get("due_date")),
            _processing_minutes(item[1]),
        )
    )
    return [job for _, job in ranked]
