from datetime import datetime, time, timedelta
from typing import Literal, Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel, Field

from ai_scheduler import rank_jobs
from auth import ALGORITHM, SECRET_KEY, create_access_token, hash_password, verify_password
from database import get_db_connection

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

MACHINE_STATUS = {"Available", "Busy", "Under Maintenance", "Breakdown"}
LEAVE_STATUS = {"Present", "On Leave"}
ATTENDANCE_SHIFT_HOURS = 8
DEFAULT_SKILLS = ["CNC", "Welding", "Assembly", "Painting", "Quality"]
DEFAULT_DEPARTMENTS = ["CNC", "Welding", "Assembly", "Painting", "Quality", "Maintenance", "Production"]


class MachineCreate(BaseModel):
    machine_id: Optional[str] = None
    machine_name: str
    purpose: str
    status: str = "Available"


class MachineStatusUpdate(BaseModel):
    status: Literal["Available", "Busy", "Under Maintenance", "Breakdown"]


class UserCreate(BaseModel):
    user_id: Optional[str] = None
    name: str
    role: Literal["worker", "supervisor"]
    skills: Optional[str] = None
    department: Optional[str] = None
    shift: str = "Day"
    in_time: str = "09:00"
    out_time: str = "17:00"
    leave_status: Literal["Present", "On Leave"] = "Present"


class AttendanceUpdate(BaseModel):
    shift: str
    skills: str
    leave_status: Literal["Present", "On Leave"]
    in_time: str = "09:00"
    out_time: str = "17:00"


class JobCreate(BaseModel):
    job_id: Optional[str] = None
    job_name: str
    processing_time: int = Field(..., gt=0)
    due_date: datetime
    priority: int = Field(..., ge=1, le=10)
    required_skill: str
    required_machine_purpose: str
    created_by: str = "Admin"


class RescheduleEvent(BaseModel):
    type: Literal["machine_breakdown", "worker_absence", "high_priority_job", "rush_job"]
    machine_id: Optional[str] = None
    worker_id: Optional[str] = None


class SupervisorWorkerIssue(BaseModel):
    worker_id: str
    reason: Optional[str] = "Reported absent/unavailable by supervisor"


class SupervisorMachineIssue(BaseModel):
    machine_id: str
    reason: Optional[str] = "Reported breakdown by supervisor"


class WorkerJobAction(BaseModel):
    source: Literal["admin", "legacy"]
    job_id: str


class WorkerIssueReport(BaseModel):
    source: Literal["admin", "legacy"]
    job_id: str
    issue_type: Literal["machine_breakdown", "job_delay"]
    machine_id: Optional[str] = None
    details: Optional[str] = "Reported by worker"


class LeaveRequest(BaseModel):
    reason: Optional[str] = "Requested by user"


def _parse_hhmm(value: str) -> time:
    raw = str(value or "").strip()
    if not raw:
        return time(9, 0)
    pieces = raw.split(":")
    if len(pieces) >= 2:
        try:
            hour = int(pieces[0])
            minute = int(pieces[1])
            hour = min(max(hour, 0), 23)
            minute = min(max(minute, 0), 59)
            return time(hour, minute)
        except ValueError:
            pass
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).time()
        except ValueError:
            continue
    return time(9, 0)


def _format_hhmm(value) -> str:
    if isinstance(value, time):
        return value.strftime("%H:%M")
    raw = str(value or "").strip()
    if not raw:
        return "09:00"
    parts = raw.split(":")
    if len(parts) >= 2:
        try:
            hour = int(parts[0])
            minute = int(parts[1])
            return f"{hour:02d}:{minute:02d}"
        except ValueError:
            pass
    return raw[:5]


def _next_id(prefix: str) -> str:
    return f"{prefix}-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[-12:]}"


def _count_rows(cursor, table_name: str) -> int:
    try:
        cursor.execute(f"SELECT COUNT(*) AS c FROM {table_name}")
        return int(cursor.fetchone()["c"])
    except Exception:
        return 0


def _table_exists(cursor, table_name: str) -> bool:
    try:
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        return cursor.fetchone() is not None
    except Exception:
        return False


def _has_legacy_data(cursor) -> bool:
    return any(
        _count_rows(cursor, name) > 0
        for name in ("jobs", "machines", "workers", "schedule")
    )


def _has_admin_data(cursor) -> bool:
    return any(
        _count_rows(cursor, name) > 0
        for name in ("admin_jobs", "admin_machines", "admin_users", "admin_schedule")
    )


def _should_use_legacy_mode(cursor) -> bool:
    # Prefer admin mode whenever admin data exists; fallback to legacy only for legacy-only setups.
    if _has_admin_data(cursor):
        return False
    return _has_legacy_data(cursor)


def _trigger_auto_reschedule(reason: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            return _build_legacy_schedule(reason)
        return _build_schedule(reason)
    finally:
        conn.close()


def _build_legacy_schedule(reschedule_reason: Optional[str] = None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    now = datetime.now()
    try:
        cursor.execute(
            """
            DELETE s
            FROM schedule s
            JOIN jobs j ON s.job_id = j.id
            WHERE j.status != 'completed'
            """
        )
        cursor.execute("UPDATE jobs SET status = 'pending' WHERE status != 'completed'")
        cursor.execute(
            """
            SELECT * FROM jobs
            WHERE status = 'pending'
            ORDER BY priority DESC, due_date ASC, duration_minutes ASC
            """
        )
        jobs = cursor.fetchall()

        cursor.execute("SELECT * FROM machines")
        machines = cursor.fetchall()
        machine_next_free = {m["id"]: now for m in machines}

        cursor.execute("SELECT * FROM schedule WHERE end_time >= %s", (now,))
        for row in cursor.fetchall():
            machine_next_free[row["machine_id"]] = max(
                machine_next_free.get(row["machine_id"], now), row["end_time"]
            )

        cursor.execute("SELECT * FROM workers")
        workers = cursor.fetchall()
        worker_next_free = {w["id"]: now for w in workers}
        jobs = rank_jobs(jobs, machines=machines, workers=workers, now=now, mode="legacy")

        for job in jobs:
            candidate_machines = [
                m
                for m in machines
                if (m.get("purpose") or "").strip().lower()
                == (job.get("required_machine_purpose") or "").strip().lower()
                and (m.get("status") or "").lower() in ("available", "busy")
            ]
            candidate_workers = [
                w
                for w in workers
                if (w.get("current_attendance") or 0) == 1
                and (w.get("status") or "").lower() == "active"
                and (job.get("required_skill") or "").strip().lower()
                in (w.get("skill_category") or "").strip().lower()
            ]

            best = None
            for machine in candidate_machines:
                machine_free = machine_next_free[machine["id"]]
                for worker in candidate_workers:
                    worker_free = worker_next_free[worker["id"]]
                    shift_start = datetime.combine(now.date(), worker["shift_start"])
                    shift_end = datetime.combine(now.date(), worker["shift_end"])
                    start_time = max(now, machine_free, worker_free, shift_start)
                    end_time = start_time + timedelta(minutes=job["duration_minutes"])
                    if end_time > shift_end:
                        continue
                    machine_idle_seconds = max(0.0, (start_time - machine_free).total_seconds())
                    candidate = (machine_idle_seconds, end_time, start_time, machine["id"], worker["id"])
                    if best is None or candidate < best:
                        best = candidate

            if best is None:
                continue

            _, end_time, start_time, machine_id, worker_id = best
            cursor.execute(
                """
                INSERT INTO schedule (job_id, machine_id, worker_id, start_time, end_time, is_active)
                VALUES (%s, %s, %s, %s, %s, 1)
                """,
                (job["id"], machine_id, worker_id, start_time, end_time),
            )
            cursor.execute("UPDATE jobs SET status = 'scheduled' WHERE id = %s", (job["id"],))
            machine_next_free[machine_id] = end_time
            worker_next_free[worker_id] = end_time

        for machine in machines:
            status = (machine.get("status") or "").lower()
            if status in ("breakdown", "under maintenance", "under_maintenance"):
                continue
            is_busy = machine_next_free[machine["id"]] > now
            next_status = "busy" if is_busy else "available"
            cursor.execute("UPDATE machines SET status = %s WHERE id = %s", (next_status, machine["id"]))

        conn.commit()
        return {"message": "Legacy schedule generated", "reschedule_reason": reschedule_reason}
    finally:
        conn.close()


def _ensure_schema() -> None:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_machines (
                id INT AUTO_INCREMENT PRIMARY KEY,
                machine_id VARCHAR(50) UNIQUE NOT NULL,
                machine_name VARCHAR(120) NOT NULL,
                purpose VARCHAR(120) NOT NULL,
                status VARCHAR(30) NOT NULL DEFAULT 'Available',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(120) NOT NULL,
                role VARCHAR(20) NOT NULL,
                skills VARCHAR(255),
                department VARCHAR(120),
                shift_name VARCHAR(50) NOT NULL DEFAULT 'Day',
                in_time TIME NOT NULL DEFAULT '09:00:00',
                out_time TIME NOT NULL DEFAULT '17:00:00',
                leave_status VARCHAR(20) NOT NULL DEFAULT 'Present',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_jobs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                job_id VARCHAR(50) UNIQUE NOT NULL,
                job_name VARCHAR(180) NOT NULL,
                processing_time INT NOT NULL,
                due_date DATETIME NOT NULL,
                priority INT NOT NULL,
                required_skill VARCHAR(120) NOT NULL,
                required_machine_purpose VARCHAR(120) NOT NULL,
                created_by VARCHAR(120) NOT NULL DEFAULT 'Admin',
                status VARCHAR(20) NOT NULL DEFAULT 'Pending',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_schedule (
                id INT AUTO_INCREMENT PRIMARY KEY,
                job_id INT NOT NULL,
                machine_id INT NOT NULL,
                worker_id INT NOT NULL,
                start_time DATETIME NOT NULL,
                end_time DATETIME NOT NULL,
                reason VARCHAR(120),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES admin_jobs(id),
                FOREIGN KEY (machine_id) REFERENCES admin_machines(id),
                FOREIGN KEY (worker_id) REFERENCES admin_users(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS supervisor_incidents (
                id INT AUTO_INCREMENT PRIMARY KEY,
                supervisor_username VARCHAR(120) NOT NULL,
                department VARCHAR(120) NOT NULL,
                issue_type VARCHAR(40) NOT NULL,
                entity_id VARCHAR(120) NOT NULL,
                details VARCHAR(255),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_incidents (
                id INT AUTO_INCREMENT PRIMARY KEY,
                worker_username VARCHAR(120) NOT NULL,
                issue_type VARCHAR(40) NOT NULL,
                source VARCHAR(20) NOT NULL,
                job_id VARCHAR(120) NOT NULL,
                machine_id VARCHAR(120),
                details VARCHAR(255),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                worker_ref VARCHAR(120) NOT NULL,
                worker_name VARCHAR(120),
                role VARCHAR(30) NOT NULL DEFAULT 'worker',
                source VARCHAR(20) NOT NULL,
                attendance_status VARCHAR(30) NOT NULL,
                in_time TIME,
                out_time TIME,
                updated_by VARCHAR(120) NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute("SHOW COLUMNS FROM attendance_logs LIKE 'role'")
        if not cursor.fetchone():
            cursor.execute(
                "ALTER TABLE attendance_logs ADD COLUMN role VARCHAR(30) NOT NULL DEFAULT 'worker' AFTER worker_name"
            )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(120) NOT NULL,
                role VARCHAR(30) NOT NULL,
                login_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                logout_time DATETIME NULL,
                source VARCHAR(20) NOT NULL DEFAULT 'auth',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


@app.on_event("startup")
def bootstrap() -> None:
    _ensure_schema()


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc

    username = payload.get("sub")
    role = payload.get("role")
    if not username or not role:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    return {"username": username, "role": role}


def require_admin(current_user=Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return current_user


def require_supervisor(current_user=Depends(get_current_user)):
    if current_user["role"] != "supervisor":
        raise HTTPException(status_code=403, detail="Supervisor role required")
    return current_user


def _department_match(value: Optional[str], department: str) -> bool:
    if not value:
        return False
    return department.strip().lower() in value.strip().lower()


def _scope_match(value: Optional[str], department: str) -> bool:
    if not value or not department:
        return False
    dept = department.strip().lower()
    tokens = [part.strip().lower() for part in str(value).replace("/", ",").split(",") if part.strip()]
    if not tokens:
        return False
    return dept in tokens


def _to_title_status(status: Optional[str]) -> str:
    if not status:
        return "Unknown"
    normalized = status.strip().lower()
    if normalized == "under_maintenance":
        normalized = "under maintenance"
    return " ".join(word.capitalize() for word in normalized.split())


def _admin_dropdown_options():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        skills = set(DEFAULT_SKILLS)
        departments = set(DEFAULT_DEPARTMENTS)
        machine_purposes = set(DEFAULT_DEPARTMENTS)

        cursor.execute("SELECT DISTINCT skills FROM admin_users WHERE skills IS NOT NULL AND skills != ''")
        for row in cursor.fetchall():
            for val in str(row["skills"]).split(","):
                clean = val.strip()
                if clean:
                    skills.add(clean)

        cursor.execute("SELECT DISTINCT department FROM admin_users WHERE department IS NOT NULL AND department != ''")
        for row in cursor.fetchall():
            departments.add(str(row["department"]).strip())

        if _table_exists(cursor, "workers"):
            cursor.execute("SELECT DISTINCT skill_category FROM workers WHERE skill_category IS NOT NULL AND skill_category != ''")
            for row in cursor.fetchall():
                skills.add(str(row["skill_category"]).strip())
                departments.add(str(row["skill_category"]).strip())

        if _table_exists(cursor, "machines"):
            cursor.execute("SELECT DISTINCT purpose FROM machines WHERE purpose IS NOT NULL AND purpose != ''")
            for row in cursor.fetchall():
                val = str(row["purpose"]).strip()
                machine_purposes.add(val)
                departments.add(val)

        cursor.execute("SELECT DISTINCT purpose FROM admin_machines WHERE purpose IS NOT NULL AND purpose != ''")
        for row in cursor.fetchall():
            val = str(row["purpose"]).strip()
            machine_purposes.add(val)
            departments.add(val)

        if _table_exists(cursor, "jobs"):
            cursor.execute("SELECT DISTINCT required_skill FROM jobs WHERE required_skill IS NOT NULL AND required_skill != ''")
            for row in cursor.fetchall():
                skills.add(str(row["required_skill"]).strip())

        cursor.execute("SELECT DISTINCT required_skill FROM admin_jobs WHERE required_skill IS NOT NULL AND required_skill != ''")
        for row in cursor.fetchall():
            skills.add(str(row["required_skill"]).strip())

        if _table_exists(cursor, "jobs"):
            cursor.execute("SELECT DISTINCT required_machine_purpose FROM jobs WHERE required_machine_purpose IS NOT NULL AND required_machine_purpose != ''")
            for row in cursor.fetchall():
                machine_purposes.add(str(row["required_machine_purpose"]).strip())

        cursor.execute("SELECT DISTINCT required_machine_purpose FROM admin_jobs WHERE required_machine_purpose IS NOT NULL AND required_machine_purpose != ''")
        for row in cursor.fetchall():
            machine_purposes.add(str(row["required_machine_purpose"]).strip())

        return {
            "skills": sorted([x for x in skills if x]),
            "departments": sorted([x for x in departments if x]),
            "machine_purposes": sorted([x for x in machine_purposes if x]),
        }
    finally:
        conn.close()


def _supervisor_department(username: str) -> str:
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT department
            FROM admin_users
            WHERE role = 'supervisor'
              AND (
                LOWER(REPLACE(name, ' ', '')) = %s
                OR LOWER(name) = %s
                OR LOWER(user_id) = %s
              )
            LIMIT 1
            """,
            (username.lower(), username.lower(), username.lower()),
        )
        row = cursor.fetchone()
        if row and row.get("department"):
            return row["department"]

        # Fallback linking for legacy/demo data: infer department from existing shop-floor records.
        inferred = None
        if _table_exists(cursor, "workers"):
            cursor.execute(
                """
                SELECT skill_category
                FROM workers
                WHERE skill_category IS NOT NULL AND skill_category != ''
                ORDER BY id ASC
                LIMIT 1
                """
            )
            w = cursor.fetchone()
            if w and w.get("skill_category"):
                inferred = w["skill_category"].strip()

        if not inferred and _table_exists(cursor, "machines"):
            cursor.execute(
                """
                SELECT purpose
                FROM machines
                WHERE purpose IS NOT NULL AND purpose != ''
                ORDER BY id ASC
                LIMIT 1
                """
            )
            m = cursor.fetchone()
            if m and m.get("purpose"):
                inferred = m["purpose"].strip()

        if not inferred:
            inferred = "Production"

        cursor.execute(
            """
            INSERT INTO admin_users (user_id, name, role, department, shift_name, in_time, out_time, leave_status)
            VALUES (%s, %s, 'supervisor', %s, 'Day', '09:00:00', '17:00:00', 'Present')
            """,
            (_next_id("SUP"), username, inferred),
        )
        conn.commit()
        return inferred
    finally:
        conn.close()


def _resolve_worker_identity(username: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        identity = {
            "username": username,
            "legacy_worker_id": None,
            "legacy_worker_name": None,
            "admin_worker_pk": None,
            "admin_worker_user_id": None,
            "admin_worker_name": None,
        }

        if _table_exists(cursor, "users"):
            cursor.execute(
                """
                SELECT associated_worker_id
                FROM users
                WHERE username = %s AND role = 'worker'
                LIMIT 1
                """,
                (username,),
            )
            legacy = cursor.fetchone()
            legacy_worker_id = legacy.get("associated_worker_id") if legacy else None
            if legacy_worker_id and _table_exists(cursor, "workers"):
                cursor.execute("SELECT name FROM workers WHERE id = %s LIMIT 1", (legacy_worker_id,))
                worker_row = cursor.fetchone()
                identity["legacy_worker_id"] = legacy_worker_id
                identity["legacy_worker_name"] = worker_row.get("name") if worker_row else None

        cursor.execute(
            """
            SELECT id, user_id, name
            FROM admin_users
            WHERE role = 'worker'
              AND (
                LOWER(REPLACE(name, ' ', '')) = %s
                OR LOWER(user_id) = %s
              )
            LIMIT 1
            """,
            (username.lower(), username.lower()),
        )
        admin_row = cursor.fetchone()
        if admin_row:
            identity["admin_worker_pk"] = admin_row["id"]
            identity["admin_worker_user_id"] = admin_row["user_id"]
            identity["admin_worker_name"] = admin_row["name"]

        if not identity["admin_worker_pk"] and _table_exists(cursor, "users"):
            cursor.execute(
                """
                SELECT id
                FROM users
                WHERE username = %s AND role = 'worker'
                LIMIT 1
                """,
                (username,),
            )
            user_row = cursor.fetchone()
            if user_row:
                generated_user_id = str(user_row["id"])
                cursor.execute(
                    """
                    INSERT INTO admin_users (user_id, name, role, skills, shift_name, in_time, out_time, leave_status)
                    VALUES (%s, %s, 'worker', %s, 'Day', '09:00:00', '17:00:00', 'Present')
                    """,
                    (generated_user_id, username, "CNC"),
                )
                conn.commit()
                cursor.execute(
                    """
                    SELECT id, user_id, name
                    FROM admin_users
                    WHERE role = 'worker'
                      AND user_id = %s
                    LIMIT 1
                    """,
                    (generated_user_id,),
                )
                admin_row = cursor.fetchone()
                if admin_row:
                    identity["admin_worker_pk"] = admin_row["id"]
                    identity["admin_worker_user_id"] = admin_row["user_id"]
                    identity["admin_worker_name"] = admin_row["name"]

        if not identity["legacy_worker_id"] and not identity["admin_worker_pk"]:
            raise HTTPException(status_code=404, detail="Worker profile not configured")
        return identity
    finally:
        conn.close()


def _find_admin_user(cursor, username: str, role: str):
    cursor.execute(
        """
        SELECT id, user_id, name, leave_status
        FROM admin_users
        WHERE role = %s
          AND (
            LOWER(REPLACE(name, ' ', '')) = %s
            OR LOWER(user_id) = %s
          )
        LIMIT 1
        """,
        (role, username.lower(), username.lower()),
    )
    return cursor.fetchone()


def _attendance_status_payload(cursor, username: str, role: str):
    cursor.execute(
        """
        SELECT id, login_time
        FROM attendance_sessions
        WHERE username = %s
          AND role = %s
          AND source = 'manual'
          AND logout_time IS NULL
        ORDER BY login_time DESC
        LIMIT 1
        """,
        (username, role),
    )
    active = cursor.fetchone()
    in_time = active["login_time"] if active else None
    expected_out = (in_time + timedelta(hours=ATTENDANCE_SHIFT_HOURS)) if in_time else None

    leave_status = "On Leave"
    profile_ref = None

    if role == "worker":
        admin_row = _find_admin_user(cursor, username, "worker")
        if admin_row:
            leave_status = admin_row.get("leave_status") or leave_status
            profile_ref = admin_row.get("user_id")
        else:
            identity = _resolve_worker_identity(username)
            if identity["legacy_worker_id"]:
                cursor.execute("SELECT current_attendance FROM workers WHERE id = %s", (identity["legacy_worker_id"],))
                legacy_row = cursor.fetchone()
                if legacy_row and int(legacy_row.get("current_attendance") or 0) == 1:
                    leave_status = "Present"
                profile_ref = str(identity["legacy_worker_id"])
    else:
        admin_row = _find_admin_user(cursor, username, "supervisor")
        if admin_row:
            leave_status = admin_row.get("leave_status") or leave_status
            profile_ref = admin_row.get("user_id")

    return {
        "active_session": bool(active),
        "in_time": in_time,
        "expected_out_time": expected_out,
        "leave_status": leave_status,
        "profile_ref": profile_ref,
    }


def _attendance_in(username: str, role: str, current_user: str):
    now = datetime.now().replace(microsecond=0)
    shift_end = now + timedelta(hours=ATTENDANCE_SHIFT_HOURS)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        status = _attendance_status_payload(cursor, username, role)
        if status["active_session"]:
            raise HTTPException(status_code=400, detail="Attendance already active. Please check out first.")

        cursor.execute(
            """
            INSERT INTO attendance_sessions (username, role, login_time, source)
            VALUES (%s, %s, %s, 'manual')
            """,
            (username, role, now),
        )

        admin_row = _find_admin_user(cursor, username, role)
        if admin_row:
            cursor.execute(
                """
                UPDATE admin_users
                SET leave_status = 'Present', in_time = %s, out_time = %s
                WHERE id = %s
                """,
                (now.time(), shift_end.time(), admin_row["id"]),
            )
            cursor.execute(
                """
                INSERT INTO attendance_logs (worker_ref, worker_name, role, source, attendance_status, in_time, out_time, updated_by)
                VALUES (%s, %s, %s, 'manual', 'Present', %s, %s, %s)
                """,
                (admin_row["user_id"], admin_row["name"], role, now.time(), shift_end.time(), current_user),
            )
        elif role == "worker":
            identity = _resolve_worker_identity(username)
            if identity["legacy_worker_id"]:
                cursor.execute(
                    """
                    UPDATE workers
                    SET current_attendance = 1, shift_start = %s, shift_end = %s, last_login = NOW()
                    WHERE id = %s
                    """,
                    (now.time(), shift_end.time(), identity["legacy_worker_id"]),
                )
                cursor.execute(
                    """
                    INSERT INTO attendance_logs (worker_ref, worker_name, role, source, attendance_status, in_time, out_time, updated_by)
                    VALUES (%s, %s, 'worker', 'legacy', 'Present', %s, %s, %s)
                    """,
                    (
                        str(identity["legacy_worker_id"]),
                        identity.get("legacy_worker_name") or username,
                        now.time(),
                        shift_end.time(),
                        current_user,
                    ),
                )
        else:
            raise HTTPException(status_code=404, detail="Supervisor profile not configured")

        conn.commit()
        return {
            "message": "Checked in",
            "in_time": now,
            "expected_out_time": shift_end,
            "shift_hours": ATTENDANCE_SHIFT_HOURS,
        }
    finally:
        conn.close()


def _attendance_out(username: str, role: str, current_user: str):
    now = datetime.now().replace(microsecond=0)
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id, login_time
            FROM attendance_sessions
            WHERE username = %s
              AND role = %s
              AND source = 'manual'
              AND logout_time IS NULL
            ORDER BY login_time DESC
            LIMIT 1
            """,
            (username, role),
        )
        active = cursor.fetchone()
        if not active:
            raise HTTPException(status_code=400, detail="No active attendance session. Please check in first.")

        cursor.execute("UPDATE attendance_sessions SET logout_time = %s WHERE id = %s", (now, active["id"]))

        admin_row = _find_admin_user(cursor, username, role)
        if admin_row:
            cursor.execute(
                """
                UPDATE admin_users
                SET leave_status = 'On Leave', out_time = %s
                WHERE id = %s
                """,
                (now.time(), admin_row["id"]),
            )
            cursor.execute(
                """
                INSERT INTO attendance_logs (worker_ref, worker_name, role, source, attendance_status, in_time, out_time, updated_by)
                VALUES (%s, %s, %s, 'manual', 'Checked Out', %s, %s, %s)
                """,
                (
                    admin_row["user_id"],
                    admin_row["name"],
                    role,
                    active["login_time"].time(),
                    now.time(),
                    current_user,
                ),
            )
        elif role == "worker":
            identity = _resolve_worker_identity(username)
            if identity["legacy_worker_id"]:
                cursor.execute(
                    """
                    UPDATE workers
                    SET current_attendance = 0, shift_end = %s
                    WHERE id = %s
                    """,
                    (now.time(), identity["legacy_worker_id"]),
                )
                cursor.execute(
                    """
                    INSERT INTO attendance_logs (worker_ref, worker_name, role, source, attendance_status, in_time, out_time, updated_by)
                    VALUES (%s, %s, 'worker', 'legacy', 'Checked Out', %s, %s, %s)
                    """,
                    (
                        str(identity["legacy_worker_id"]),
                        identity.get("legacy_worker_name") or username,
                        active["login_time"].time(),
                        now.time(),
                        current_user,
                    ),
                )

        conn.commit()
        return {"message": "Checked out", "out_time": now}
    finally:
        conn.close()


def _attendance_leave(username: str, role: str, current_user: str, reason: str):
    now = datetime.now().replace(microsecond=0)
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            UPDATE attendance_sessions
            SET logout_time = %s
            WHERE username = %s
              AND role = %s
              AND source = 'manual'
              AND logout_time IS NULL
            """,
            (now, username, role),
        )

        admin_row = _find_admin_user(cursor, username, role)
        if admin_row:
            cursor.execute(
                """
                UPDATE admin_users
                SET leave_status = 'On Leave'
                WHERE id = %s
                """,
                (admin_row["id"],),
            )
            cursor.execute(
                """
                INSERT INTO attendance_logs (worker_ref, worker_name, role, source, attendance_status, in_time, out_time, updated_by)
                VALUES (%s, %s, %s, 'manual', %s, NULL, NULL, %s)
                """,
                (admin_row["user_id"], admin_row["name"], role, "Leave Requested", current_user),
            )
        elif role == "worker":
            identity = _resolve_worker_identity(username)
            if identity["legacy_worker_id"]:
                cursor.execute(
                    "UPDATE workers SET current_attendance = 0 WHERE id = %s",
                    (identity["legacy_worker_id"],),
                )
                cursor.execute(
                    """
                    INSERT INTO attendance_logs (worker_ref, worker_name, role, source, attendance_status, in_time, out_time, updated_by)
                    VALUES (%s, %s, 'worker', 'legacy', %s, NULL, NULL, %s)
                    """,
                    (
                        str(identity["legacy_worker_id"]),
                        identity.get("legacy_worker_name") or username,
                        "Leave Requested",
                        current_user,
                    ),
                )

        conn.commit()
    finally:
        conn.close()
    _trigger_auto_reschedule("worker_absence")
    return {"message": "Leave request submitted"}


def _build_schedule(reschedule_reason: Optional[str] = None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    now = datetime.now()

    try:
        cursor.execute(
            """
            DELETE s
            FROM admin_schedule s
            JOIN admin_jobs j ON s.job_id = j.id
            WHERE j.status != 'Completed'
            """
        )
        cursor.execute("UPDATE admin_jobs SET status = 'Pending' WHERE status != 'Completed'")
        cursor.execute(
            """
            SELECT * FROM admin_jobs
            WHERE status = 'Pending'
            ORDER BY priority DESC, due_date ASC, processing_time ASC
            """
        )
        jobs = cursor.fetchall()

        cursor.execute("SELECT * FROM admin_machines")
        machines = cursor.fetchall()
        machine_by_id = {m["id"]: m for m in machines}
        machine_next_free = {m["id"]: now for m in machines}

        cursor.execute("SELECT * FROM admin_schedule WHERE end_time >= %s", (now,))
        for row in cursor.fetchall():
            machine_next_free[row["machine_id"]] = max(machine_next_free.get(row["machine_id"], now), row["end_time"])

        cursor.execute("SELECT * FROM admin_users WHERE role = 'worker'")
        workers = cursor.fetchall()
        worker_next_free = {w["id"]: now for w in workers}
        jobs = rank_jobs(jobs, machines=machines, workers=workers, now=now, mode="admin")

        for job in jobs:
            candidate_machines = [
                m
                for m in machines
                if m["purpose"].strip().lower() == job["required_machine_purpose"].strip().lower()
                and m["status"] in ("Available", "Busy")
            ]
            candidate_workers = []
            for worker in workers:
                worker_skills = (worker.get("skills") or "").lower()
                if worker["leave_status"] != "Present":
                    continue
                if job["required_skill"].strip().lower() not in worker_skills:
                    continue
                candidate_workers.append(worker)

            best = None
            for machine in candidate_machines:
                machine_free = machine_next_free[machine["id"]]
                for worker in candidate_workers:
                    worker_free = worker_next_free[worker["id"]]
                    shift_start = datetime.combine(
                        now.date(),
                        worker["in_time"] if isinstance(worker["in_time"], time) else _parse_hhmm(str(worker["in_time"])[:5]),
                    )
                    shift_end = datetime.combine(
                        now.date(),
                        worker["out_time"] if isinstance(worker["out_time"], time) else _parse_hhmm(str(worker["out_time"])[:5]),
                    )
                    start_time = max(now, machine_free, worker_free, shift_start)
                    end_time = start_time + timedelta(minutes=job["processing_time"])
                    if end_time > shift_end:
                        continue
                    machine_idle_seconds = max(0.0, (start_time - machine_free).total_seconds())
                    candidate = (machine_idle_seconds, end_time, start_time, machine["id"], worker["id"])
                    if best is None or candidate < best:
                        best = candidate

            if best is None:
                continue

            _, end_time, start_time, machine_id, worker_id = best
            cursor.execute(
                """
                INSERT INTO admin_schedule (job_id, machine_id, worker_id, start_time, end_time, reason)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (job["id"], machine_id, worker_id, start_time, end_time, reschedule_reason),
            )
            cursor.execute("UPDATE admin_jobs SET status = 'Scheduled' WHERE id = %s", (job["id"],))
            machine_next_free[machine_id] = end_time
            worker_next_free[worker_id] = end_time

        for machine in machine_by_id.values():
            if machine["status"] in ("Breakdown", "Under Maintenance"):
                continue
            is_busy = machine_next_free[machine["id"]] > now
            next_status = "Busy" if is_busy else "Available"
            cursor.execute("UPDATE admin_machines SET status = %s WHERE id = %s", (next_status, machine["id"]))

        conn.commit()
        return {"message": "Schedule generated", "reschedule_reason": reschedule_reason}
    finally:
        conn.close()


@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM users WHERE username = %s", (form_data.username,))
    user = cursor.fetchone()

    if not user:
        conn.close()
        raise HTTPException(status_code=400, detail="Username not found")

    if not verify_password(form_data.password, user["password"]):
        conn.close()
        raise HTTPException(status_code=400, detail="Password mismatch")

    token = create_access_token(data={"sub": user["username"], "role": user["role"]})
    conn.close()

    return {
        "access_token": token,
        "token_type": "bearer",
        "role": user["role"],
        "username": user["username"],
    }


@app.post("/logout")
def logout(current_user=Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE attendance_sessions
            SET logout_time = NOW()
            WHERE username = %s
              AND role = %s
              AND logout_time IS NULL
            ORDER BY login_time DESC
            LIMIT 1
            """,
            (current_user["username"], current_user["role"]),
        )

        if current_user["role"] in ("worker", "supervisor"):
            cursor.execute(
                """
                UPDATE admin_users
                SET leave_status = 'On Leave'
                WHERE role = %s
                  AND (
                    LOWER(REPLACE(name, ' ', '')) = %s
                    OR LOWER(user_id) = %s
                  )
                """,
                (current_user["role"], current_user["username"].lower(), current_user["username"].lower()),
            )
        if current_user["role"] == "worker":
            cursor.execute(
                "UPDATE workers SET current_attendance = 0 WHERE id IN (SELECT associated_worker_id FROM users WHERE username = %s)",
                (current_user["username"],),
            )
        conn.commit()
        return {"message": "Logged out"}
    finally:
        conn.close()


@app.get("/admin/dashboard", dependencies=[Depends(require_admin)])
def admin_dashboard():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if not _should_use_legacy_mode(cursor):
            cursor.execute("SELECT COUNT(*) AS total_jobs FROM admin_jobs")
            total_jobs = cursor.fetchone()["total_jobs"]
            cursor.execute("SELECT COUNT(*) AS pending_jobs FROM admin_jobs WHERE status = 'Pending'")
            pending_jobs = cursor.fetchone()["pending_jobs"]
            cursor.execute("SELECT COUNT(*) AS active_machines FROM admin_machines WHERE status = 'Available'")
            active_machines = cursor.fetchone()["active_machines"]
            cursor.execute("SELECT COUNT(*) AS total_workers FROM admin_users WHERE role = 'worker'")
            total_workers = cursor.fetchone()["total_workers"]
        else:
            cursor.execute("SELECT COUNT(*) AS total_jobs FROM jobs")
            total_jobs = cursor.fetchone()["total_jobs"]
            cursor.execute("SELECT COUNT(*) AS pending_jobs FROM jobs WHERE status = 'pending'")
            pending_jobs = cursor.fetchone()["pending_jobs"]
            cursor.execute("SELECT COUNT(*) AS active_machines FROM machines WHERE status = 'available'")
            active_machines = cursor.fetchone()["active_machines"]
            cursor.execute("SELECT COUNT(*) AS total_workers FROM workers")
            total_workers = cursor.fetchone()["total_workers"]
        return {
            "total_jobs": total_jobs,
            "pending_jobs": pending_jobs,
            "active_machines": active_machines,
            "total_workers": total_workers,
        }
    finally:
        conn.close()


@app.get("/admin/options", dependencies=[Depends(require_admin)])
def admin_options():
    return _admin_dropdown_options()


@app.post("/admin/machines", dependencies=[Depends(require_admin)])
def add_machine(machine: MachineCreate):
    if machine.status not in MACHINE_STATUS:
        raise HTTPException(status_code=400, detail="Invalid machine status")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        machine_id = machine.machine_id or _next_id("MCH")
        cursor.execute(
            """
            INSERT INTO admin_machines (machine_id, machine_name, purpose, status)
            VALUES (%s, %s, %s, %s)
            """,
            (machine_id, machine.machine_name, machine.purpose, machine.status),
        )
        conn.commit()
        return {"message": "Machine added", "machine_id": machine_id}
    finally:
        conn.close()


@app.get("/admin/machines", dependencies=[Depends(require_admin)])
def list_machines():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            cursor.execute(
                """
                SELECT
                    id,
                    CAST(id AS CHAR) AS machine_id,
                    name AS machine_name,
                    purpose,
                    CASE
                        WHEN LOWER(status) = 'available' THEN 'Available'
                        WHEN LOWER(status) = 'busy' THEN 'Busy'
                        WHEN LOWER(status) IN ('under maintenance', 'under_maintenance') THEN 'Under Maintenance'
                        WHEN LOWER(status) = 'breakdown' THEN 'Breakdown'
                        ELSE status
                    END AS status
                FROM machines
                ORDER BY id DESC
                """
            )
            return cursor.fetchall()

        cursor.execute("SELECT * FROM admin_machines ORDER BY created_at DESC")
        return cursor.fetchall()
    finally:
        conn.close()


@app.patch("/admin/machines/{machine_id}/status", dependencies=[Depends(require_admin)])
def update_machine_status(machine_id: str, body: MachineStatusUpdate):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id FROM admin_machines WHERE machine_id = %s", (machine_id,))
        machine = cursor.fetchone()
        if machine:
            cursor.execute("UPDATE admin_machines SET status = %s WHERE machine_id = %s", (body.status, machine_id))
        else:
            cursor.execute("UPDATE machines SET status = %s WHERE id = %s", (body.status.lower(), machine_id))
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Machine not found")
        conn.commit()
        if _should_use_legacy_mode(cursor):
            _build_legacy_schedule("machine_breakdown" if body.status in ("Breakdown", "Under Maintenance") else "resource_update")
        else:
            _build_schedule("machine_breakdown" if body.status in ("Breakdown", "Under Maintenance") else "resource_update")
        return {"message": "Machine status updated"}
    finally:
        conn.close()


@app.post("/admin/users", dependencies=[Depends(require_admin)])
def create_user(user: UserCreate):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        user_id = user.user_id or _next_id("USR")
        in_time = _parse_hhmm(user.in_time)
        out_time = _parse_hhmm(user.out_time)

        if user.role == "worker" and not user.skills:
            raise HTTPException(status_code=400, detail="Worker skills are required")
        if user.role == "supervisor" and not user.department:
            raise HTTPException(status_code=400, detail="Supervisor department is required")

        cursor.execute(
            """
            INSERT INTO admin_users (user_id, name, role, skills, department, shift_name, in_time, out_time, leave_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                user.name,
                user.role,
                user.skills,
                user.department,
                user.shift,
                in_time,
                out_time,
                user.leave_status,
            ),
        )

        username = user.name.lower().replace(" ", "")
        cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
        existing_login = cursor.fetchone()
        if not existing_login:
            cursor.execute(
                """
                INSERT INTO users (username, password, role, associated_worker_id)
                VALUES (%s, %s, %s, NULL)
                """,
                (username, hash_password("123"), user.role),
            )

        conn.commit()
        return {"message": f"User added. Login: {username} / 123", "user_id": user_id}
    finally:
        conn.close()


@app.delete("/admin/users/{user_id}", dependencies=[Depends(require_admin)])
def delete_user(user_id: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id, name, role FROM admin_users WHERE user_id = %s", (user_id,))
        admin_row = cursor.fetchone()
        if admin_row:
            cursor.execute("DELETE FROM admin_users WHERE id = %s", (admin_row["id"],))
            username = admin_row["name"].lower().replace(" ", "")
            cursor.execute("DELETE FROM users WHERE username = %s AND role = %s", (username, admin_row["role"]))
            conn.commit()
            return {"message": "User deleted"}

        if not user_id.isdigit():
            raise HTTPException(status_code=404, detail="User not found")

        cursor.execute("SELECT id, role, associated_worker_id FROM users WHERE id = %s", (int(user_id),))
        legacy_row = cursor.fetchone()
        if not legacy_row:
            raise HTTPException(status_code=404, detail="User not found")
        if legacy_row["role"] == "admin":
            raise HTTPException(status_code=400, detail="Admin login cannot be deleted from this screen")

        cursor.execute("DELETE FROM users WHERE id = %s", (legacy_row["id"],))
        if legacy_row.get("associated_worker_id"):
            cursor.execute("DELETE FROM workers WHERE id = %s", (legacy_row["associated_worker_id"],))
        conn.commit()
        return {"message": "User deleted"}
    finally:
        conn.close()


@app.get("/admin/users", dependencies=[Depends(require_admin)])
def list_users():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            cursor.execute(
                """
                SELECT
                    u.id,
                    CAST(u.id AS CHAR) AS user_id,
                    COALESCE(w.name, u.username) AS name,
                    u.role,
                    w.skill_category AS skills,
                    NULL AS department,
                    'Day' AS shift_name,
                    COALESCE(w.shift_start, '09:00:00') AS in_time,
                    COALESCE(w.shift_end, '17:00:00') AS out_time,
                    CASE WHEN COALESCE(w.current_attendance, 0) = 1 THEN 'Present' ELSE 'On Leave' END AS leave_status,
                    NOW() AS created_at
                FROM users u
                LEFT JOIN workers w ON u.associated_worker_id = w.id
                """
            )
            return cursor.fetchall()

        cursor.execute("SELECT * FROM admin_users ORDER BY created_at DESC")
        return cursor.fetchall()
    finally:
        conn.close()


@app.get("/admin/attendance", dependencies=[Depends(require_admin)])
def get_attendance():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            cursor.execute(
                """
                SELECT
                    w.id,
                    CAST(w.id AS CHAR) AS user_id,
                    w.name,
                    'worker' AS role,
                    w.skill_category AS skills,
                    'Day' AS shift_name,
                    w.shift_start AS in_time,
                    w.shift_end AS out_time,
                    CASE WHEN w.current_attendance = 1 THEN 'Present' ELSE 'On Leave' END AS leave_status,
                    NOW() AS created_at
                FROM workers w
                ORDER BY w.name ASC
                """
            )
            return cursor.fetchall()

        cursor.execute("SELECT * FROM admin_users WHERE role IN ('worker', 'supervisor') ORDER BY name ASC")
        return cursor.fetchall()
    finally:
        conn.close()


@app.get("/admin/attendance-records", dependencies=[Depends(require_admin)])
def attendance_records():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT
                CONCAT('session-', id) AS record_id,
                username,
                role,
                login_time,
                logout_time,
                source,
                CASE
                    WHEN logout_time IS NULL THEN 'Checked In'
                    ELSE 'Checked Out'
                END AS status
            FROM attendance_sessions
            UNION ALL
            SELECT
                CONCAT('log-', id) AS record_id,
                worker_ref AS username,
                role,
                CASE
                    WHEN in_time IS NULL THEN created_at
                    ELSE TIMESTAMP(DATE(created_at), in_time)
                END AS login_time,
                CASE
                    WHEN out_time IS NULL THEN NULL
                    ELSE TIMESTAMP(DATE(created_at), out_time)
                END AS logout_time,
                source,
                attendance_status AS status
            FROM attendance_logs
            ORDER BY login_time DESC
            LIMIT 2000
            """
        )
        return cursor.fetchall()
    finally:
        conn.close()


@app.put("/admin/attendance/{worker_id}", dependencies=[Depends(require_admin)])
def update_attendance(worker_id: str, body: AttendanceUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE admin_users
            SET shift_name = %s, skills = %s, leave_status = %s, in_time = %s, out_time = %s
            WHERE user_id = %s AND role = 'worker'
            """,
            (body.shift, body.skills, body.leave_status, _parse_hhmm(body.in_time), _parse_hhmm(body.out_time), worker_id),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                """
                UPDATE workers
                SET shift_start = %s, shift_end = %s, skill_category = %s, current_attendance = %s
                WHERE id = %s
                """,
                (
                    _parse_hhmm(body.in_time),
                    _parse_hhmm(body.out_time),
                    body.skills,
                    1 if body.leave_status == "Present" else 0,
                    worker_id,
                ),
            )
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Worker not found")
            cursor.execute(
                """
                INSERT INTO attendance_logs (worker_ref, worker_name, source, attendance_status, in_time, out_time, updated_by)
                SELECT CAST(id AS CHAR), name, 'legacy', %s, shift_start, shift_end, 'admin'
                FROM workers
                WHERE id = %s
                """,
                (body.leave_status, worker_id),
            )
        else:
            cursor.execute(
                """
                INSERT INTO attendance_logs (worker_ref, worker_name, source, attendance_status, in_time, out_time, updated_by)
                SELECT user_id, name, 'admin', %s, in_time, out_time, 'admin'
                FROM admin_users
                WHERE user_id = %s
                """,
                (body.leave_status, worker_id),
            )
        conn.commit()
        conn2 = get_db_connection()
        cursor2 = conn2.cursor(dictionary=True)
        try:
            if _should_use_legacy_mode(cursor2):
                _build_legacy_schedule("worker_absence" if body.leave_status == "On Leave" else "resource_update")
            else:
                _build_schedule("worker_absence" if body.leave_status == "On Leave" else "resource_update")
        finally:
            conn2.close()
        return {"message": "Attendance updated"}
    finally:
        conn.close()


@app.post("/admin/jobs", dependencies=[Depends(require_admin)])
def create_job(job: JobCreate):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        job_id = job.job_id or _next_id("JOB")
        cursor.execute(
            """
            INSERT INTO admin_jobs
            (job_id, job_name, processing_time, due_date, priority, required_skill, required_machine_purpose, created_by, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Pending')
            """,
            (
                job_id,
                job.job_name,
                job.processing_time,
                job.due_date,
                job.priority,
                job.required_skill,
                job.required_machine_purpose,
                job.created_by,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    now = datetime.now()
    hours_until_due = (job.due_date - now).total_seconds() / 3600.0
    is_rush = job.priority >= 9 or hours_until_due <= 12
    _trigger_auto_reschedule("rush_job" if is_rush else "new_job")
    return {"message": "Job created", "job_id": job_id}


@app.get("/admin/jobs", dependencies=[Depends(require_admin)])
def list_jobs():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            cursor.execute(
                """
                SELECT
                    j.id,
                    CAST(j.id AS CHAR) AS job_id,
                    COALESCE(j.description, j.client_name) AS job_name,
                    j.duration_minutes AS processing_time,
                    j.due_date,
                    j.priority,
                    j.required_skill,
                    j.required_machine_purpose,
                    'Admin' AS created_by,
                    CONCAT(UCASE(LEFT(j.status, 1)), LCASE(SUBSTRING(j.status, 2))) AS status,
                    j.created_at
                FROM jobs j
                ORDER BY j.priority DESC, j.due_date ASC
                """
            )
            return cursor.fetchall()

        cursor.execute("SELECT * FROM admin_jobs ORDER BY priority DESC, due_date ASC")
        return cursor.fetchall()
    finally:
        conn.close()


@app.post("/admin/generate-schedule", dependencies=[Depends(require_admin)])
def generate_schedule():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            return _build_legacy_schedule()
        return _build_schedule()
    finally:
        conn.close()


@app.post("/schedule", dependencies=[Depends(require_admin)])
def generate_schedule_compat():
    return generate_schedule()


@app.post("/admin/reschedule", dependencies=[Depends(require_admin)])
def dynamic_reschedule(event: RescheduleEvent):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        has_legacy = _should_use_legacy_mode(conn.cursor(dictionary=True))
        if event.type == "machine_breakdown":
            if not event.machine_id:
                raise HTTPException(status_code=400, detail="machine_id required")
            if has_legacy:
                cursor.execute("UPDATE machines SET status = 'breakdown' WHERE id = %s", (event.machine_id,))
            else:
                cursor.execute("UPDATE admin_machines SET status = 'Breakdown' WHERE machine_id = %s", (event.machine_id,))
        elif event.type == "worker_absence":
            if not event.worker_id:
                raise HTTPException(status_code=400, detail="worker_id required")
            if has_legacy:
                cursor.execute("UPDATE workers SET current_attendance = 0 WHERE id = %s", (event.worker_id,))
            else:
                cursor.execute("UPDATE admin_users SET leave_status = 'On Leave' WHERE user_id = %s", (event.worker_id,))
        conn.commit()
    finally:
        conn.close()

    if event.type in ("high_priority_job", "rush_job"):
        return generate_schedule()
    return _trigger_auto_reschedule(event.type)


@app.get("/admin/schedule", dependencies=[Depends(require_admin)])
def get_schedule():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            cursor.execute(
                """
                SELECT
                    s.id,
                    CAST(j.id AS CHAR) AS job_id,
                    COALESCE(j.description, j.client_name) AS job_name,
                    j.priority,
                    CAST(m.id AS CHAR) AS machine_id,
                    m.name AS machine_name,
                    CAST(w.id AS CHAR) AS worker_id,
                    w.name AS worker_name,
                    s.start_time,
                    s.end_time,
                    'legacy' AS reason
                FROM schedule s
                JOIN jobs j ON s.job_id = j.id
                JOIN machines m ON s.machine_id = m.id
                JOIN workers w ON s.worker_id = w.id
                ORDER BY j.priority DESC, s.start_time ASC
                """
            )
            return cursor.fetchall()

        cursor.execute(
            """
            SELECT
                s.id,
                j.job_id,
                j.job_name,
                j.priority,
                m.machine_id,
                m.machine_name,
                u.user_id AS worker_id,
                u.name AS worker_name,
                s.start_time,
                s.end_time,
                s.reason
            FROM admin_schedule s
            JOIN admin_jobs j ON s.job_id = j.id
            JOIN admin_machines m ON s.machine_id = m.id
            JOIN admin_users u ON s.worker_id = u.id
            ORDER BY j.priority DESC, s.start_time ASC
            """
        )
        return cursor.fetchall()
    finally:
        conn.close()


@app.get("/admin/reports", dependencies=[Depends(require_admin)])
def reports():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            cursor.execute("SELECT COUNT(*) AS jobs_scheduled FROM schedule")
            jobs_scheduled = cursor.fetchone()["jobs_scheduled"]
            cursor.execute(
                """
                SELECT COALESCE(SUM(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 0) AS machine_busy_minutes
                FROM schedule
                """
            )
            busy_minutes = cursor.fetchone()["machine_busy_minutes"]
            cursor.execute("SELECT COUNT(*) AS machine_count FROM machines")
            machine_count = cursor.fetchone()["machine_count"]
            cursor.execute("SELECT COUNT(*) AS worker_count FROM workers")
            worker_count = cursor.fetchone()["worker_count"]
            cursor.execute(
                """
                SELECT
                    s.id,
                    CAST(j.id AS CHAR) AS job_id,
                    COALESCE(j.description, j.client_name) AS job_name,
                    m.name AS machine_name,
                    w.name AS worker_name,
                    s.start_time,
                    s.end_time
                FROM schedule s
                JOIN jobs j ON s.job_id = j.id
                JOIN machines m ON s.machine_id = m.id
                JOIN workers w ON s.worker_id = w.id
                ORDER BY s.start_time ASC
                """
            )
            gantt_rows = cursor.fetchall()
        else:
            cursor.execute("SELECT COUNT(*) AS jobs_scheduled FROM admin_schedule")
            jobs_scheduled = cursor.fetchone()["jobs_scheduled"]
            cursor.execute(
                """
                SELECT COALESCE(SUM(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 0) AS machine_busy_minutes
                FROM admin_schedule
                """
            )
            busy_minutes = cursor.fetchone()["machine_busy_minutes"]
            cursor.execute("SELECT COUNT(*) AS machine_count FROM admin_machines")
            machine_count = cursor.fetchone()["machine_count"]
            cursor.execute("SELECT COUNT(*) AS worker_count FROM admin_users WHERE role = 'worker'")
            worker_count = cursor.fetchone()["worker_count"]
            cursor.execute(
                """
                SELECT
                    s.id,
                    j.job_id,
                    j.job_name,
                    m.machine_name,
                    u.name AS worker_name,
                    s.start_time,
                    s.end_time
                FROM admin_schedule s
                JOIN admin_jobs j ON s.job_id = j.id
                JOIN admin_machines m ON s.machine_id = m.id
                JOIN admin_users u ON s.worker_id = u.id
                ORDER BY s.start_time ASC
                """
            )
            gantt_rows = cursor.fetchall()

        return {
            "jobs_scheduled": jobs_scheduled,
            "machine_busy_minutes": busy_minutes,
            "machine_count": machine_count,
            "worker_count": worker_count,
            "gantt": gantt_rows,
        }
    finally:
        conn.close()


@app.get("/admin/schedule-view", dependencies=[Depends(require_admin)])
def schedule_view_compat():
    return get_schedule()


def _supervisor_workers_rows(department: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []
    try:
        if _should_use_legacy_mode(cursor):
            if _table_exists(cursor, "workers"):
                cursor.execute("SELECT * FROM workers ORDER BY name ASC")
                for row in cursor.fetchall():
                    if not _scope_match(row.get("skill_category"), department):
                        continue
                    is_present = int(row.get("current_attendance") or 0) == 1
                    rows.append(
                        {
                            "worker_id": str(row["id"]),
                            "name": row["name"],
                            "skills": row.get("skill_category") or "-",
                            "shift": "Day",
                            "in_time": _format_hhmm(row.get("shift_start", "09:00:00")),
                            "out_time": _format_hhmm(row.get("shift_end", "17:00:00")),
                            "availability": "Available" if is_present else "Unavailable",
                            "attendance_status": "Present" if is_present else "On Leave",
                            "department": department,
                        }
                    )
            return rows

        cursor.execute("SELECT * FROM admin_users WHERE role = 'worker' ORDER BY name ASC")
        for row in cursor.fetchall():
            if row.get("department"):
                if not _scope_match(row["department"], department):
                    continue
            elif not _scope_match(row.get("skills"), department):
                continue
            leave_status = row.get("leave_status", "Present")
            rows.append(
                {
                    "worker_id": row["user_id"],
                    "name": row["name"],
                    "skills": row.get("skills") or "-",
                    "shift": row.get("shift_name") or "Day",
                    "in_time": _format_hhmm(row.get("in_time", "09:00:00")),
                    "out_time": _format_hhmm(row.get("out_time", "17:00:00")),
                    "availability": "Available" if leave_status == "Present" else "Unavailable",
                    "attendance_status": leave_status,
                    "department": row.get("department") or department,
                }
            )
        return rows
    finally:
        conn.close()


def _supervisor_machines_rows(department: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []
    try:
        if _should_use_legacy_mode(cursor):
            if _table_exists(cursor, "machines"):
                cursor.execute("SELECT * FROM machines ORDER BY id DESC")
                for row in cursor.fetchall():
                    if not _scope_match(row.get("purpose"), department):
                        continue
                    rows.append(
                        {
                            "machine_id": str(row["id"]),
                            "machine_name": row["name"],
                            "purpose": row["purpose"],
                            "status": _to_title_status(row.get("status")),
                        }
                    )
            return rows

        cursor.execute("SELECT * FROM admin_machines ORDER BY created_at DESC")
        for row in cursor.fetchall():
            if not _scope_match(row.get("purpose"), department):
                continue
            rows.append(
                {
                    "machine_id": row["machine_id"],
                    "machine_name": row["machine_name"],
                    "purpose": row["purpose"],
                    "status": _to_title_status(row.get("status")),
                }
            )
        return rows
    finally:
        conn.close()


def _supervisor_jobs_rows(department: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []
    now = datetime.now()
    try:
        if _should_use_legacy_mode(cursor):
            if _table_exists(cursor, "jobs") and _table_exists(cursor, "machines") and _table_exists(cursor, "workers") and _table_exists(cursor, "schedule"):
                cursor.execute(
                    """
                    SELECT
                        j.id AS job_id,
                        COALESCE(j.description, j.client_name) AS job_name,
                        j.duration_minutes AS processing_time,
                        j.due_date,
                        j.priority,
                        j.required_skill,
                        j.required_machine_purpose,
                        j.status,
                        m.name AS assigned_machine,
                        w.name AS assigned_worker,
                        s.start_time,
                        s.end_time
                    FROM jobs j
                    LEFT JOIN schedule s ON s.job_id = j.id
                    LEFT JOIN machines m ON s.machine_id = m.id
                    LEFT JOIN workers w ON s.worker_id = w.id
                    ORDER BY j.priority DESC, j.due_date ASC
                    """
                )
                for row in cursor.fetchall():
                    if not (
                        _scope_match(row.get("required_machine_purpose"), department)
                        or _scope_match(row.get("required_skill"), department)
                    ):
                        continue
                    status = _to_title_status(row.get("status"))
                    if row.get("start_time") and row.get("end_time"):
                        if row["start_time"] <= now <= row["end_time"]:
                            status = "Running"
                        elif row["end_time"] < now and status != "Completed":
                            status = "Delayed"
                        elif row["start_time"] > now and status != "Completed":
                            status = "Scheduled"
                    rows.append(
                        {
                            "job_id": str(row["job_id"]),
                            "job_name": row["job_name"],
                            "processing_time": row["processing_time"],
                            "due_date": row["due_date"],
                            "priority": row["priority"],
                            "assigned_machine": row.get("assigned_machine") or "-",
                            "assigned_worker": row.get("assigned_worker") or "-",
                            "start_time": row.get("start_time"),
                            "end_time": row.get("end_time"),
                            "status": status,
                        }
                    )
            return rows

        cursor.execute(
            """
            SELECT
                j.job_id,
                j.job_name,
                j.processing_time,
                j.due_date,
                j.priority,
                j.required_skill,
                j.required_machine_purpose,
                j.status,
                m.machine_name AS assigned_machine,
                u.name AS assigned_worker,
                s.start_time,
                s.end_time
            FROM admin_jobs j
            LEFT JOIN admin_schedule s ON s.job_id = j.id
            LEFT JOIN admin_machines m ON s.machine_id = m.id
            LEFT JOIN admin_users u ON s.worker_id = u.id
            ORDER BY j.priority DESC, j.due_date ASC
            """
        )
        for row in cursor.fetchall():
            if not (
                _scope_match(row.get("required_machine_purpose"), department)
                or _scope_match(row.get("required_skill"), department)
            ):
                continue
            status = _to_title_status(row.get("status"))
            if row.get("start_time") and row.get("end_time"):
                if row["start_time"] <= now <= row["end_time"]:
                    status = "Running"
                elif row["end_time"] < now and status != "Completed":
                    status = "Delayed"
                elif row["start_time"] > now and status != "Completed":
                    status = "Scheduled"
            rows.append(
                {
                    "job_id": row["job_id"],
                    "job_name": row["job_name"],
                    "processing_time": row["processing_time"],
                    "due_date": row["due_date"],
                    "priority": row["priority"],
                    "assigned_machine": row.get("assigned_machine") or "-",
                    "assigned_worker": row.get("assigned_worker") or "-",
                    "start_time": row.get("start_time"),
                    "end_time": row.get("end_time"),
                    "status": status,
                }
            )
        return rows
    finally:
        conn.close()


@app.get("/supervisor/profile", dependencies=[Depends(require_supervisor)])
def supervisor_profile(current_user=Depends(require_supervisor)):
    department = _supervisor_department(current_user["username"])
    return {"username": current_user["username"], "department": department}


@app.get("/supervisor/workers", dependencies=[Depends(require_supervisor)])
def supervisor_workers(current_user=Depends(require_supervisor)):
    department = _supervisor_department(current_user["username"])
    return _supervisor_workers_rows(department)


@app.get("/supervisor/machines", dependencies=[Depends(require_supervisor)])
def supervisor_machines(current_user=Depends(require_supervisor)):
    department = _supervisor_department(current_user["username"])
    return _supervisor_machines_rows(department)


@app.get("/supervisor/jobs", dependencies=[Depends(require_supervisor)])
def supervisor_jobs(current_user=Depends(require_supervisor)):
    department = _supervisor_department(current_user["username"])
    return _supervisor_jobs_rows(department)


@app.get("/supervisor/schedule", dependencies=[Depends(require_supervisor)])
def supervisor_schedule(current_user=Depends(require_supervisor)):
    department = _supervisor_department(current_user["username"])
    return _supervisor_jobs_rows(department)


@app.get("/supervisor/attendance-history", dependencies=[Depends(require_supervisor)])
def supervisor_attendance_history(current_user=Depends(require_supervisor)):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT
                CONCAT('session-', id) AS record_id,
                username,
                role,
                login_time,
                logout_time,
                source,
                CASE
                    WHEN logout_time IS NULL THEN 'Checked In'
                    ELSE 'Checked Out'
                END AS status
            FROM attendance_sessions
            WHERE username = %s
              AND role = 'supervisor'
            UNION ALL
            SELECT
                CONCAT('log-', id) AS record_id,
                worker_ref AS username,
                role,
                CASE
                    WHEN in_time IS NULL THEN created_at
                    ELSE TIMESTAMP(DATE(created_at), in_time)
                END AS login_time,
                CASE
                    WHEN out_time IS NULL THEN NULL
                    ELSE TIMESTAMP(DATE(created_at), out_time)
                END AS logout_time,
                source,
                attendance_status AS status
            FROM attendance_logs
            WHERE worker_ref = %s
              AND role = 'supervisor'
            ORDER BY login_time DESC
            LIMIT 500
            """,
            (current_user["username"], current_user["username"]),
        )
        return cursor.fetchall()
    finally:
        conn.close()


@app.get("/supervisor/attendance-status", dependencies=[Depends(require_supervisor)])
def supervisor_attendance_status(current_user=Depends(require_supervisor)):
    _supervisor_department(current_user["username"])
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        return _attendance_status_payload(cursor, current_user["username"], "supervisor")
    finally:
        conn.close()


@app.post("/supervisor/attendance/in", dependencies=[Depends(require_supervisor)])
def supervisor_attendance_in(current_user=Depends(require_supervisor)):
    _supervisor_department(current_user["username"])
    return _attendance_in(current_user["username"], "supervisor", current_user["username"])


@app.post("/supervisor/attendance/out", dependencies=[Depends(require_supervisor)])
def supervisor_attendance_out(current_user=Depends(require_supervisor)):
    return _attendance_out(current_user["username"], "supervisor", current_user["username"])


@app.post("/supervisor/attendance/leave", dependencies=[Depends(require_supervisor)])
def supervisor_attendance_leave(payload: LeaveRequest, current_user=Depends(require_supervisor)):
    return _attendance_leave(current_user["username"], "supervisor", current_user["username"], payload.reason or "Requested by user")


@app.get("/supervisor/dashboard", dependencies=[Depends(require_supervisor)])
def supervisor_dashboard(current_user=Depends(require_supervisor)):
    department = _supervisor_department(current_user["username"])
    workers = _supervisor_workers_rows(department)
    jobs = _supervisor_jobs_rows(department)
    machines = _supervisor_machines_rows(department)

    active_jobs = sum(1 for row in jobs if row["status"] in ("Scheduled", "Running"))
    completed_jobs = sum(1 for row in jobs if row["status"] == "Completed")
    delayed_jobs = sum(1 for row in jobs if row["status"] == "Delayed")
    machine_overview = {
        "Available": sum(1 for row in machines if row["status"] == "Available"),
        "Busy": sum(1 for row in machines if row["status"] == "Busy"),
        "Under Maintenance": sum(1 for row in machines if row["status"] == "Under Maintenance"),
        "Breakdown": sum(1 for row in machines if row["status"] == "Breakdown"),
    }

    return {
        "department": department,
        "total_workers": len(workers),
        "active_jobs": active_jobs,
        "completed_jobs": completed_jobs,
        "delayed_jobs": delayed_jobs,
        "machine_status_overview": machine_overview,
    }


@app.post("/supervisor/report-worker-absence", dependencies=[Depends(require_supervisor)])
def supervisor_report_worker_absence(payload: SupervisorWorkerIssue, current_user=Depends(require_supervisor)):
    department = _supervisor_department(current_user["username"])
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE admin_users SET leave_status = 'On Leave' WHERE user_id = %s AND role = 'worker'",
            (payload.worker_id,),
        )
        if cursor.rowcount == 0:
            cursor.execute("UPDATE workers SET current_attendance = 0 WHERE id = %s", (payload.worker_id,))
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Worker not found")
        cursor.execute(
            """
            INSERT INTO supervisor_incidents (supervisor_username, department, issue_type, entity_id, details)
            VALUES (%s, %s, 'worker_absence', %s, %s)
            """,
            (current_user["username"], department, payload.worker_id, payload.reason),
        )
        conn.commit()
    finally:
        conn.close()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            return _build_legacy_schedule("worker_absence")
        return _build_schedule("worker_absence")
    finally:
        conn.close()


@app.post("/supervisor/report-machine-breakdown", dependencies=[Depends(require_supervisor)])
def supervisor_report_machine_breakdown(payload: SupervisorMachineIssue, current_user=Depends(require_supervisor)):
    department = _supervisor_department(current_user["username"])
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE admin_machines SET status = 'Breakdown' WHERE machine_id = %s", (payload.machine_id,))
        if cursor.rowcount == 0:
            cursor.execute("UPDATE machines SET status = 'breakdown' WHERE id = %s", (payload.machine_id,))
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Machine not found")
        cursor.execute(
            """
            INSERT INTO supervisor_incidents (supervisor_username, department, issue_type, entity_id, details)
            VALUES (%s, %s, 'machine_breakdown', %s, %s)
            """,
            (current_user["username"], department, payload.machine_id, payload.reason),
        )
        conn.commit()
    finally:
        conn.close()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if _should_use_legacy_mode(cursor):
            return _build_legacy_schedule("machine_breakdown")
        return _build_schedule("machine_breakdown")
    finally:
        conn.close()


def require_worker(current_user=Depends(get_current_user)):
    if current_user["role"] != "worker":
        raise HTTPException(status_code=403, detail="Worker role required")
    return current_user


@app.get("/worker/profile", dependencies=[Depends(require_worker)])
def worker_profile(current_user=Depends(require_worker)):
    identity = _resolve_worker_identity(current_user["username"])
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if identity["admin_worker_pk"]:
            cursor.execute("SELECT * FROM admin_users WHERE id = %s", (identity["admin_worker_pk"],))
            row = cursor.fetchone()
            return {
                "worker_id": row["user_id"],
                "name": row["name"],
                "skills": row.get("skills") or "-",
                "shift": row.get("shift_name") or "Day",
                "in_time": _format_hhmm(row.get("in_time", "09:00:00")),
                "out_time": _format_hhmm(row.get("out_time", "17:00:00")),
                "availability": "Available" if row.get("leave_status") == "Present" else "Unavailable",
            }

        cursor.execute("SELECT * FROM workers WHERE id = %s", (identity["legacy_worker_id"],))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Worker not found")
        return {
            "worker_id": str(row["id"]),
            "name": row["name"],
            "skills": row.get("skill_category") or "-",
            "shift": "Day",
            "in_time": _format_hhmm(row.get("shift_start", "09:00:00")),
            "out_time": _format_hhmm(row.get("shift_end", "17:00:00")),
            "availability": "Available" if int(row.get("current_attendance") or 0) == 1 else "Unavailable",
        }
    finally:
        conn.close()


@app.get("/worker/jobs", dependencies=[Depends(require_worker)])
def worker_jobs(current_user=Depends(require_worker)):
    identity = _resolve_worker_identity(current_user["username"])
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []
    now = datetime.now()
    try:
        if identity["admin_worker_pk"]:
            cursor.execute(
                """
                SELECT
                    'admin' AS source,
                    j.job_id,
                    j.job_name,
                    j.processing_time,
                    j.due_date,
                    j.priority,
                    m.machine_id,
                    m.machine_name,
                    m.purpose AS machine_purpose,
                    s.start_time,
                    s.end_time,
                    j.status
                FROM admin_schedule s
                JOIN admin_jobs j ON s.job_id = j.id
                JOIN admin_machines m ON s.machine_id = m.id
                WHERE s.worker_id = %s
                ORDER BY s.start_time DESC
                """,
                (identity["admin_worker_pk"],),
            )
            for row in cursor.fetchall():
                status = _to_title_status(row.get("status"))
                if row["start_time"] <= now <= row["end_time"] and status != "Completed":
                    status = "Running"
                rows.append({**row, "status": status})

        if identity["legacy_worker_id"]:
            cursor.execute(
                """
                SELECT
                    'legacy' AS source,
                    CAST(j.id AS CHAR) AS job_id,
                    COALESCE(j.description, j.client_name) AS job_name,
                    j.duration_minutes AS processing_time,
                    j.due_date,
                    j.priority,
                    CAST(m.id AS CHAR) AS machine_id,
                    m.name AS machine_name,
                    m.purpose AS machine_purpose,
                    s.start_time,
                    s.end_time,
                    j.status
                FROM schedule s
                JOIN jobs j ON s.job_id = j.id
                JOIN machines m ON s.machine_id = m.id
                WHERE s.worker_id = %s
                ORDER BY s.start_time DESC
                """,
                (identity["legacy_worker_id"],),
            )
            for row in cursor.fetchall():
                status = _to_title_status(row.get("status"))
                if row["start_time"] <= now <= row["end_time"] and status != "Completed":
                    status = "Running"
                rows.append({**row, "status": status})

        return rows
    finally:
        conn.close()


def _validate_worker_job_access(identity, payload: WorkerJobAction):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if payload.source == "admin":
            if not identity["admin_worker_pk"]:
                raise HTTPException(status_code=403, detail="No access to this admin job")
            cursor.execute(
                """
                SELECT j.id
                FROM admin_jobs j
                JOIN admin_schedule s ON s.job_id = j.id
                WHERE j.job_id = %s AND s.worker_id = %s
                LIMIT 1
                """,
                (payload.job_id, identity["admin_worker_pk"]),
            )
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Assigned job not found")
            return row["id"]

        if not identity["legacy_worker_id"]:
            raise HTTPException(status_code=403, detail="No access to this legacy job")
        cursor.execute(
            """
            SELECT j.id
            FROM jobs j
            JOIN schedule s ON s.job_id = j.id
            WHERE j.id = %s AND s.worker_id = %s
            LIMIT 1
            """,
            (payload.job_id, identity["legacy_worker_id"]),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Assigned job not found")
        return row["id"]
    finally:
        conn.close()


@app.post("/worker/jobs/start", dependencies=[Depends(require_worker)])
def worker_start_job(payload: WorkerJobAction, current_user=Depends(require_worker)):
    identity = _resolve_worker_identity(current_user["username"])
    internal_job_id = _validate_worker_job_access(identity, payload)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if payload.source == "admin":
            cursor.execute("UPDATE admin_jobs SET status = 'Running' WHERE id = %s", (internal_job_id,))
        else:
            cursor.execute("UPDATE jobs SET status = 'running' WHERE id = %s", (internal_job_id,))
        conn.commit()
        return {"message": "Job started"}
    finally:
        conn.close()


@app.post("/worker/jobs/complete", dependencies=[Depends(require_worker)])
def worker_complete_job(payload: WorkerJobAction, current_user=Depends(require_worker)):
    identity = _resolve_worker_identity(current_user["username"])
    internal_job_id = _validate_worker_job_access(identity, payload)
    now = datetime.now()

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if payload.source == "admin":
            cursor.execute("SELECT machine_id FROM admin_schedule WHERE job_id = %s LIMIT 1", (internal_job_id,))
            machine_row = cursor.fetchone()
            cursor.execute("UPDATE admin_jobs SET status = 'Completed' WHERE id = %s", (internal_job_id,))
            cursor.execute("UPDATE admin_schedule SET end_time = %s WHERE job_id = %s", (now, internal_job_id))
            if machine_row:
                machine_id = machine_row[0]
                cursor.execute(
                    """
                    SELECT COUNT(*) 
                    FROM admin_schedule s
                    JOIN admin_jobs j ON s.job_id = j.id
                    WHERE s.machine_id = %s
                      AND s.start_time <= %s
                      AND s.end_time > %s
                      AND j.status != 'Completed'
                    """,
                    (machine_id, now, now),
                )
                active_count = int(cursor.fetchone()[0])
                cursor.execute(
                    "UPDATE admin_machines SET status = %s WHERE id = %s",
                    ("Busy" if active_count > 0 else "Available", machine_id),
                )
        else:
            cursor.execute("SELECT machine_id FROM schedule WHERE job_id = %s LIMIT 1", (internal_job_id,))
            machine_row = cursor.fetchone()
            cursor.execute("UPDATE jobs SET status = 'completed' WHERE id = %s", (internal_job_id,))
            cursor.execute("UPDATE schedule SET end_time = %s, is_active = 0 WHERE job_id = %s", (now, internal_job_id))
            if machine_row:
                machine_id = machine_row[0]
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM schedule s
                    JOIN jobs j ON s.job_id = j.id
                    WHERE s.machine_id = %s
                      AND s.start_time <= %s
                      AND s.end_time > %s
                      AND j.status != 'completed'
                    """,
                    (machine_id, now, now),
                )
                active_count = int(cursor.fetchone()[0])
                cursor.execute(
                    "UPDATE machines SET status = %s WHERE id = %s",
                    ("busy" if active_count > 0 else "available", machine_id),
                )
        conn.commit()
        return {"message": "Job marked as completed"}
    finally:
        conn.close()


@app.post("/worker/report-issue", dependencies=[Depends(require_worker)])
def worker_report_issue(payload: WorkerIssueReport, current_user=Depends(require_worker)):
    identity = _resolve_worker_identity(current_user["username"])
    _validate_worker_job_access(identity, WorkerJobAction(source=payload.source, job_id=payload.job_id))

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if payload.issue_type == "job_delay":
            if payload.source == "admin":
                cursor.execute("UPDATE admin_jobs SET status = 'Delayed' WHERE job_id = %s", (payload.job_id,))
            else:
                cursor.execute("UPDATE jobs SET status = 'delayed' WHERE id = %s", (payload.job_id,))
        elif payload.issue_type == "machine_breakdown":
            if not payload.machine_id:
                raise HTTPException(status_code=400, detail="machine_id required for machine breakdown")
            cursor.execute("UPDATE admin_machines SET status = 'Breakdown' WHERE machine_id = %s", (payload.machine_id,))
            if cursor.rowcount == 0:
                cursor.execute("UPDATE machines SET status = 'breakdown' WHERE id = %s", (payload.machine_id,))

        cursor.execute(
            """
            INSERT INTO worker_incidents (worker_username, issue_type, source, job_id, machine_id, details)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                current_user["username"],
                payload.issue_type,
                payload.source,
                payload.job_id,
                payload.machine_id,
                payload.details,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    if payload.issue_type == "machine_breakdown":
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            if _should_use_legacy_mode(cursor):
                _build_legacy_schedule("machine_breakdown")
            else:
                _build_schedule("machine_breakdown")
        finally:
            conn.close()
    return {"message": "Issue reported successfully"}


@app.get("/worker/attendance-history", dependencies=[Depends(require_worker)])
def worker_attendance_history(current_user=Depends(require_worker)):
    identity = _resolve_worker_identity(current_user["username"])
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    history = []
    try:
        cursor.execute(
            """
            SELECT
                CONCAT('session-', id) AS record_id,
                username AS worker_ref,
                username AS worker_name,
                role,
                'session' AS source,
                CASE WHEN logout_time IS NULL THEN 'Present' ELSE 'Logged Out' END AS attendance_status,
                login_time AS in_time,
                logout_time AS out_time,
                'auth' AS updated_by,
                login_time AS created_at
            FROM attendance_sessions
            WHERE username = %s AND role = 'worker'
            ORDER BY login_time DESC
            LIMIT 500
            """,
            (current_user["username"],),
        )
        history.extend(cursor.fetchall())

        refs = []
        if identity["admin_worker_user_id"]:
            refs.append(identity["admin_worker_user_id"])
        if identity["legacy_worker_id"]:
            refs.append(str(identity["legacy_worker_id"]))

        for ref in refs:
            cursor.execute(
                """
                SELECT worker_ref, worker_name, source, attendance_status, in_time, out_time, updated_by, created_at
                FROM attendance_logs
                WHERE worker_ref = %s
                ORDER BY created_at DESC
                """,
                (ref,),
            )
            history.extend(cursor.fetchall())

        if not history:
            if identity["legacy_worker_id"]:
                cursor.execute(
                    """
                    SELECT
                        CAST(id AS CHAR) AS worker_ref,
                        name AS worker_name,
                        'worker' AS role,
                        'legacy' AS source,
                        CASE WHEN current_attendance = 1 THEN 'Present' ELSE 'On Leave' END AS attendance_status,
                        shift_start AS in_time,
                        shift_end AS out_time,
                        'admin' AS updated_by,
                        COALESCE(last_login, NOW()) AS created_at
                    FROM workers
                    WHERE id = %s
                    """,
                    (identity["legacy_worker_id"],),
                )
                row = cursor.fetchone()
                if row:
                    history.append(row)
            elif identity["admin_worker_user_id"]:
                cursor.execute(
                    """
                    SELECT
                        user_id AS worker_ref,
                        name AS worker_name,
                        role,
                        'admin' AS source,
                        leave_status AS attendance_status,
                        in_time,
                        out_time,
                        'admin' AS updated_by,
                        created_at
                    FROM admin_users
                    WHERE id = %s
                    """,
                    (identity["admin_worker_pk"],),
                )
                row = cursor.fetchone()
                if row:
                    history.append(row)

        return history
    finally:
        conn.close()


@app.get("/worker/attendance-status", dependencies=[Depends(require_worker)])
def worker_attendance_status(current_user=Depends(require_worker)):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        return _attendance_status_payload(cursor, current_user["username"], "worker")
    finally:
        conn.close()


@app.post("/worker/attendance/in", dependencies=[Depends(require_worker)])
def worker_attendance_in(current_user=Depends(require_worker)):
    return _attendance_in(current_user["username"], "worker", current_user["username"])


@app.post("/worker/attendance/out", dependencies=[Depends(require_worker)])
def worker_attendance_out(current_user=Depends(require_worker)):
    return _attendance_out(current_user["username"], "worker", current_user["username"])


@app.post("/worker/attendance/leave", dependencies=[Depends(require_worker)])
def worker_attendance_leave(payload: LeaveRequest, current_user=Depends(require_worker)):
    return _attendance_leave(current_user["username"], "worker", current_user["username"], payload.reason or "Requested by user")
