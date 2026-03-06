import json
import os
import time
from datetime import datetime, timezone
import sqlite3
import requests


JOB_SUBMITTED_EVENT = "job_submitted"
JOB_RESULT_SUBMITTED_EVENT = "job_result_submitted"

# Weekly pool and reward cadence
WEEKLY_POINTS = 2_000_000
WINDOW_SECONDS = 1800
WINDOW_POINTS = WEEKLY_POINTS / (7 * 24 * 2)

# Tuple timeout: (connect_timeout, read_timeout)
HTTP_TIMEOUT = (5, 30)

# How many times to retry a page request
HTTP_RETRIES = 4

# Indexing tuneables
WINDOW_RETRIES = 2
REQUEST_TIMEOUT = 60
LOOP_SLEEP_SECONDS = 10


# -----------------------------
# Utils
# -----------------------------
def cosmos_time_to_unix(ts: str) -> int:
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1]

    if "." in ts:
        date_part, frac = ts.split(".", 1)
        frac_digits = "".join(ch for ch in frac if ch.isdigit())
        frac_us = (frac_digits + "000000")[:6]
        ts = f"{date_part}.{frac_us}"

    dt = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def find_job_id(events: list) -> int:
    for event in events or []:
        for attr in event.get("attributes", []) or []:
            key = str(attr.get("key", "")).lower()
            if key == "job_id":
                try:
                    return int(attr.get("value"))
                except Exception:
                    return 0
    return 0


def height_windows(start: int, end: int, chunk: int):
    h = start
    while h <= end:
        yield h, min(h + chunk - 1, end)
        h += chunk


def get_latest_block_height(api_url: str) -> int:
    resp = requests.get(
        f"{api_url}/cosmos/base/tendermint/v1beta1/blocks/latest",
        timeout=REQUEST_TIMEOUT,
    ).json()
    return int(resp["block"]["header"]["height"])


def save_config(config_dict: dict):
    with open("config.json", "w") as cfg_file:
        json.dump(config_dict, cfg_file, indent=4, sort_keys=False)


def shorten_validator(addr: str) -> str:
    if len(addr) <= 18:
        return addr
    return f"{addr[:11]}...{addr[-4:]}"


def get_latest_closed_reward_window(now_ts: int | None = None) -> tuple[int, int]:
    if now_ts is None:
        now_ts = int(time.time())

    window_end = (now_ts // WINDOW_SECONDS) * WINDOW_SECONDS
    window_start = window_end - WINDOW_SECONDS
    return window_start, window_end


# -----------------------------
# DB ops
# -----------------------------
def job_exists(db: sqlite3.Connection, job_id: int) -> bool:
    cur = db.execute("SELECT 1 FROM jobs WHERE job_id = ?", [job_id])
    return cur.fetchone() is not None


def submit_job(
    db: sqlite3.Connection,
    job_id: int,
    submission_txhash: str,
    submission_height: int,
    submission_timestamp: int,
    creator: str,
    target_validator: str,
    fee: str,
):
    db.execute(
        """
        INSERT OR IGNORE INTO jobs(
            job_id,
            submission_txhash,
            submission_height,
            submission_timestamp,
            creator,
            target_validator,
            fee
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            job_id,
            submission_txhash,
            submission_height,
            submission_timestamp,
            creator,
            target_validator,
            fee,
        ],
    )


def submit_job_result(
    db: sqlite3.Connection,
    job_id: int,
    result_txhash: str,
    result_height: int,
    result_timestamp: int,
):
    db.execute(
        """
        UPDATE jobs SET
            result_txhash = ?,
            result_height = ?,
            result_timestamp = ?
        WHERE job_id = ?
        """,
        [result_txhash, result_height, result_timestamp, job_id],
    )


def get_validator_points(db: sqlite3.Connection, validator_address: str) -> float:
    cur = db.execute(
        "SELECT pp FROM points WHERE validator_address = ?",
        [validator_address]
    )
    row = cur.fetchone()
    return float(row[0]) if row else 0.0


def add_validator_points(db: sqlite3.Connection, validator_address: str, points_delta: float):
    db.execute(
        """
        INSERT INTO points(validator_address, pp)
        VALUES(?, ?)
        ON CONFLICT(validator_address)
        DO UPDATE SET pp = pp + excluded.pp
        """,
        [validator_address, points_delta]
    )


def reward_window_exists(db: sqlite3.Connection, window_start: int) -> bool:
    cur = db.execute(
        "SELECT 1 FROM reward_windows WHERE window_start = ?",
        [window_start]
    )
    return cur.fetchone() is not None


def get_last_rewarded_window_start(db: sqlite3.Connection) -> int | None:
    cur = db.execute("SELECT MAX(window_start) FROM reward_windows")
    row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def save_reward_window(
    db: sqlite3.Connection,
    window_start: int,
    window_end: int,
    total_jobs_completed: int,
    total_points_distributed: float
):
    db.execute(
        """
        INSERT OR IGNORE INTO reward_windows(
            window_start,
            window_end,
            total_jobs_completed,
            total_points_distributed,
            created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            window_start,
            window_end,
            total_jobs_completed,
            total_points_distributed,
            int(time.time())
        ]
    )


# -----------------------------
# HTTP
# -----------------------------
def http_get_json(url: str, params: dict) -> dict:
    last_err = None
    for attempt in range(HTTP_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep_s = 0.4 * (2 ** attempt)
            print(f"[HTTP] error={type(e).__name__}: {e} | retry in {sleep_s:.1f}s")
            time.sleep(sleep_s)
    raise last_err


# -----------------------------
# Fetching
# -----------------------------
def fetch_all_txs_offset_with_total(
    api_url: str,
    query: str,
    page_limit: int
):
    all_txs = []
    offset = 0
    total = None
    page = 0
    first_txhash_seen_at_offset0 = None

    while True:
        params = {
            "query": query,
            "pagination.limit": page_limit,
            "pagination.offset": offset,
            "pagination.count_total": "true",
        }

        resp = http_get_json(f"{api_url}/cosmos/tx/v1beta1/txs", params)
        txs = resp.get("tx_responses") or []

        if total is None:
            total_raw = (resp.get("pagination") or {}).get("total")
            total = int(total_raw) if total_raw is not None else None

        page += 1
        if page == 1 and txs:
            first_txhash_seen_at_offset0 = txs[0].get("txhash")

        if (
            offset > 0
            and txs
            and first_txhash_seen_at_offset0
            and txs[0].get("txhash") == first_txhash_seen_at_offset0
        ):
            print("[PAGING] Node ignores pagination.offset. Falling back to cursor pagination.")
            return fetch_all_txs_cursor(api_url, query, page_limit)

        all_txs.extend(txs)
        offset += page_limit

        if not txs:
            break
        if total is not None and offset >= total:
            break

    return all_txs, total


def fetch_all_txs_cursor(
    api_url: str,
    query: str,
    page_limit: int
):
    all_txs = []
    seen = set()
    next_key = None
    total = None

    while True:
        params = {
            "query": query,
            "pagination.limit": page_limit,
            "pagination.count_total": "true",
        }
        if next_key:
            params["pagination.key"] = next_key

        resp = http_get_json(f"{api_url}/cosmos/tx/v1beta1/txs", params)
        txs = resp.get("tx_responses") or []

        if total is None:
            total_raw = (resp.get("pagination") or {}).get("total")
            total = int(total_raw) if total_raw is not None else None

        last_next_key = next_key
        next_key = (resp.get("pagination") or {}).get("next_key")

        for tx in txs:
            txhash = tx.get("txhash")
            if txhash and txhash not in seen:
                seen.add(txhash)
                all_txs.append(tx)

        if not next_key or next_key == last_next_key:
            break

    return all_txs, total


def fetch_window_all(
    api_url: str,
    query: str,
    page_limit: int,
    retries: int = WINDOW_RETRIES
):
    last = ([], None)

    for attempt in range(retries + 1):
        txs, total = fetch_all_txs_offset_with_total(api_url, query, page_limit)
        last = (txs, total)

        if total is None or len(txs) == total:
            return txs

        print(f"[WARN] Incomplete fetch got={len(txs)} expected={total} attempt={attempt + 1}/{retries}")
        time.sleep(0.6)

    txs, total = last
    if total is not None and len(txs) != total:
        print(f"[WARN] Still incomplete after retries got={len(txs)} expected={total}")
    return txs


# -----------------------------
# Indexing
# -----------------------------
def index_submitted_jobs(
    api_url: str,
    db: sqlite3.Connection,
    start_height: int,
    end_height: int,
    page_limit: int,
    window: int
):
    for a, b in height_windows(start_height, end_height, window):
        print(f"[JOBS] fetching window {a}-{b} ...")
        q = f"{JOB_SUBMITTED_EVENT}.job_id EXISTS AND tx.height >= {a} AND tx.height <= {b}"
        txs = fetch_window_all(api_url, q, page_limit)

        added = 0
        for tx in txs:
            if tx.get("code") != 0:
                continue

            job_id = find_job_id(tx.get("events", []))
            if job_id == 0 or job_exists(db, job_id):
                continue

            submission_txhash = tx["txhash"]
            submission_height = int(tx["height"])
            submission_timestamp = cosmos_time_to_unix(tx["timestamp"])

            msg = tx["tx"]["body"]["messages"][0]
            creator = msg["creator"]
            target_validator = msg["target_validator"]
            fee = msg["fee"]["amount"] + " " + msg["fee"]["denom"]

            submit_job(
                db,
                job_id,
                submission_txhash,
                submission_height,
                submission_timestamp,
                creator,
                target_validator,
                fee,
            )
            added += 1

        db.commit()
        print(f"[JOBS] window {a}-{b} | txs={len(txs)} | added={added}")


def index_submitted_job_results(
    api_url: str,
    db: sqlite3.Connection,
    start_height: int,
    end_height: int,
    page_limit: int,
    window: int
):
    for a, b in height_windows(start_height, end_height, window):
        print(f"[RESULTS] fetching window {a}-{b} ...")
        q = f"{JOB_RESULT_SUBMITTED_EVENT}.job_id EXISTS AND tx.height >= {a} AND tx.height <= {b}"
        txs = fetch_window_all(api_url, q, page_limit)

        updated = 0
        for tx in txs:
            if tx.get("code") != 0:
                continue

            job_id = find_job_id(tx.get("events", []))
            if job_id == 0:
                continue

            result_txhash = tx["txhash"]
            result_height = int(tx["height"])
            result_timestamp = cosmos_time_to_unix(tx["timestamp"])

            submit_job_result(
                db,
                job_id,
                result_txhash,
                result_height,
                result_timestamp
            )
            updated += 1

        db.commit()
        print(f"[RESULTS] window {a}-{b} | txs={len(txs)} | updated={updated}")


# -----------------------------
# Leaderboards / rewards
# -----------------------------
def create_leaderboard_for_window(
    db: sqlite3.Connection,
    window_start: int,
    window_end: int
):
    query = """
        SELECT
          target_validator,
          COUNT(submission_txhash) AS jobs_assigned,
          SUM(CASE WHEN result_txhash IS NOT NULL THEN 1 ELSE 0 END) AS jobs_completed,
          AVG(CASE
                WHEN result_txhash IS NOT NULL AND result_timestamp IS NOT NULL
                THEN (result_timestamp - submission_timestamp)
              END) AS avg_completion_seconds
        FROM jobs
        WHERE result_timestamp IS NOT NULL
          AND result_timestamp >= ?
          AND result_timestamp < ?
        GROUP BY target_validator
        HAVING jobs_completed > 0
        ORDER BY jobs_completed DESC, jobs_assigned DESC, target_validator ASC;
    """
    cur = db.execute(query, [window_start, window_end])
    return cur.fetchall()


def format_points_leaderboard_message(rows: list[tuple], window_start: int, window_end: int) -> str:
    if not rows:
        return (
            "🏅**Top miners** (updated every 30 minutes)\n"
            "No completed jobs in this reward window."
        )

    lines = [
        "🏅 **Top validators** (updated every 30 minutes)",
    ]

    for idx, row in enumerate(rows, start=1):
        validator, jobs_assigned, jobs_completed, avg_completion_seconds, total_pp, gained_pp = row

        avg_sec = int(round(avg_completion_seconds or 0))
        total_pp_int = int(round(total_pp))
        gained_pp_int = int(round(gained_pp))

        lines.append(
            f"**{idx}**. `{shorten_validator(validator)}` "
            f"(**{jobs_assigned}** jobs assigned, **{jobs_completed}** jobs executed, avg. compute time: **{avg_sec}s**) "
            f"– **{total_pp_int}**pp (+*{gained_pp_int}* points)"
        )

    return "\n".join(lines)


def post_discord_message(bot_token: str, channel_id: int, content: str):
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"content": content}

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()


def reward_window(db: sqlite3.Connection, config: dict, window_start: int, window_end: int):
    if reward_window_exists(db, window_start):
        print(f"[REWARDS] Window {window_start}-{window_end} already processed.")
        return

    leaderboard = create_leaderboard_for_window(db, window_start, window_end)
    total_jobs_completed = sum(row[2] for row in leaderboard)

    if total_jobs_completed == 0:
        save_reward_window(db, window_start, window_end, 0, 0.0)
        db.commit()
        print(f"[REWARDS] No completed jobs in window {window_start}-{window_end}.")
        return

    enriched_rows = []

    for validator, jobs_assigned, jobs_completed, avg_completion_seconds in leaderboard:
        gained_pp = WINDOW_POINTS * (jobs_completed / total_jobs_completed)
        add_validator_points(db, validator, gained_pp)
        total_pp = get_validator_points(db, validator)

        enriched_rows.append(
            (
                validator,
                jobs_assigned,
                jobs_completed,
                avg_completion_seconds,
                total_pp,
                gained_pp,
            )
        )

    save_reward_window(
        db,
        window_start,
        window_end,
        total_jobs_completed,
        WINDOW_POINTS,
    )
    db.commit()

    message = format_points_leaderboard_message(enriched_rows, window_start, window_end)

    if config.get("discord_bot_token") and config.get("discord_channel_id"):
        post_discord_message(
            config["discord_bot_token"],
            int(config["discord_channel_id"]),
            message,
        )

    print(
        f"[REWARDS] Distributed {WINDOW_POINTS:.2f} PP for window "
        f"{window_start}-{window_end} across {total_jobs_completed} completed jobs."
    )


def reward_all_missed_closed_windows(db: sqlite3.Connection, config: dict):
    latest_closed_start, _ = get_latest_closed_reward_window()

    last_rewarded_start = get_last_rewarded_window_start(db)

    if last_rewarded_start is None:
        first_window_to_process = latest_closed_start
    else:
        first_window_to_process = last_rewarded_start + WINDOW_SECONDS

    if first_window_to_process > latest_closed_start:
        print("[REWARDS] No missed reward windows.")
        return

    current_start = first_window_to_process
    while current_start <= latest_closed_start:
        current_end = current_start + WINDOW_SECONDS
        reward_window(db, config, current_start, current_end)
        current_start += WINDOW_SECONDS


# -----------------------------
# Config / DB init
# -----------------------------
def is_valid_config(config_dict: dict) -> bool:
    if "api_url" not in config_dict or (
        not config_dict["api_url"].startswith("http://")
        and not config_dict["api_url"].startswith("https://")
    ):
        print(
            "`api_url` parameter is missing a proper URL! "
            "Make sure that URL starts with `http://...` or `https://...`"
        )
        return False

    config_dict["api_url"] = config_dict["api_url"].rstrip("/")

    if "db_path" not in config_dict or not config_dict["db_path"].endswith(".db"):
        print(
            "`db_path` parameter is missing a proper SQLite DB filepath! "
            "Make sure that filepath ends with `.db` extension."
        )
        return False

    if "page_limit" not in config_dict or config_dict["page_limit"] <= 0:
        print("`page_limit` is invalid. Using default value `100`.")
        config_dict["page_limit"] = 100

    if "block_window_length" not in config_dict or config_dict["block_window_length"] <= 0:
        print("`block_window_length` is invalid. Using default value `250`.")
        config_dict["block_window_length"] = 250

    if "last_indexed_height" not in config_dict:
        print("`last_indexed_height` missing. Using default value `1`.")
        config_dict["last_indexed_height"] = 1

    if "loop_sleep_seconds" not in config_dict or config_dict["loop_sleep_seconds"] < 0:
        print(f"`loop_sleep_seconds` is invalid. Using default value `{LOOP_SLEEP_SECONDS}`.")
        config_dict["loop_sleep_seconds"] = LOOP_SLEEP_SECONDS

    if "discord_bot_token" not in config_dict:
        config_dict["discord_bot_token"] = ""

    if "discord_channel_id" not in config_dict:
        config_dict["discord_channel_id"] = 0

    return True


def initialize_sqlite(db_path: str) -> sqlite3.Connection:
    db = sqlite3.connect(db_path)

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs(
            job_id INTEGER PRIMARY KEY,
            submission_txhash TEXT NOT NULL,
            submission_height INTEGER NOT NULL,
            submission_timestamp INTEGER NOT NULL,
            result_txhash TEXT,
            result_height INTEGER,
            result_timestamp INTEGER,
            creator TEXT NOT NULL,
            target_validator TEXT NOT NULL,
            fee TEXT NOT NULL
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS points(
            validator_address TEXT PRIMARY KEY,
            pp REAL NOT NULL DEFAULT 0
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS reward_windows(
            window_start INTEGER PRIMARY KEY,
            window_end INTEGER NOT NULL,
            total_jobs_completed INTEGER NOT NULL,
            total_points_distributed REAL NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )

    db.commit()
    return db


# -----------------------------
# Main cycle
# -----------------------------
def run_indexing_cycle(config: dict, db: sqlite3.Connection):
    last_indexed_height = config["last_indexed_height"]
    current_latest_height = get_latest_block_height(config["api_url"])

    if current_latest_height < last_indexed_height:
        print(
            f"[WARN] current_latest_height ({current_latest_height}) < "
            f"last_indexed_height ({last_indexed_height}). Skipping cycle."
        )
        return

    if current_latest_height > last_indexed_height:
        print(f"Indexing transactions from blocks #{last_indexed_height} to #{current_latest_height}...")

        start_time = int(time.time())

        index_submitted_jobs(
            config["api_url"],
            db,
            last_indexed_height,
            current_latest_height,
            config["page_limit"],
            config["block_window_length"]
        )
        index_submitted_job_results(
            config["api_url"],
            db,
            last_indexed_height,
            current_latest_height,
            config["page_limit"],
            config["block_window_length"]
        )

        end_time = int(time.time())
        print(f"Indexing cycle completed in {end_time - start_time} sec.!")

        config["last_indexed_height"] = current_latest_height + 1
        save_config(config)
        print(f"[INFO] Saved progress at height {config['last_indexed_height']}")
    else:
        print(f"[INFO] No new blocks yet. Current height: {current_latest_height}")

    reward_all_missed_closed_windows(db, config)


# -----------------------------
# Main
# -----------------------------
def main():
    if not os.path.exists("config.json"):
        print(
            "Configuration file does not exist! "
            "Make sure you copied `config.json.example` and populated all blank values."
        )
        raise SystemExit(1)

    with open("config.json", "r") as cfg_file:
        config = json.load(cfg_file)

    if not is_valid_config(config):
        print(
            "`config.json` contains invalid parameter values! "
            "Make sure you copied `config.json.example` and populated all parameter values correctly."
        )
        raise SystemExit(1)

    db = initialize_sqlite(config["db_path"])

    print("[INFO] Continuous indexing started. Press Ctrl+C to stop.")

    try:
        while True:
            try:
                run_indexing_cycle(config, db)
            except Exception as e:
                print(f"[ERROR] Indexing cycle failed: {type(e).__name__}: {e}")

            time.sleep(config["loop_sleep_seconds"])

    except KeyboardInterrupt:
        print("\n[INFO] Ctrl+C received. Shutting down gracefully...")
        save_config(config)
        db.commit()
        db.close()
        print("[INFO] Progress saved. Bye.")


if __name__ == "__main__":
    main()