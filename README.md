# Republic Testnet

Indexer and reward distribution service for the **Republic Testnet** validator job system.

This service continuously indexes on-chain jobs and results, calculates validator performance every **30 minutes**, distributes **reward points**, and publishes a leaderboard to **Discord**.

Validator addresses follow the format:

```
raivaloper...
```

---

# Overview

The service performs three main tasks:

1. **Index chain transactions**
   - Detects `job_submitted` events
   - Detects `job_result_submitted` events
   - Stores all job data locally in SQLite

2. **Compute validator performance**
   - Calculates statistics every **30 minutes**
   - Metrics include:
     - jobs assigned
     - jobs completed
     - average compute time

3. **Distribute reward points**
   - Weekly pool: **2,000,000 points**
   - Distributed proportionally based on **jobs executed**
   - Reward cycle: **every 30 minutes**

Results are posted automatically into a **Discord channel**.

---

# Reward Logic

Total weekly pool:

```
2,000,000 points
```

Total windows per week:

```
7 days × 24 hours × 2 windows/hour = 336 windows
```

Points distributed per window:

```
~5952.38 points
```

Distribution formula:

```
validator_points = window_points * (validator_jobs_completed / total_jobs_completed)
```

Example leaderboard message:

```
🏅 Top validators (updated every 30 minutes)

1. raivaloper...29fs (10 jobs assigned, 10 jobs executed, avg. compute time: 25s) – 2000pp (+148 points)
2. raivaloper...f82k (8 jobs assigned, 6 jobs executed, avg. compute time: 57s) – 1480pp (+90 points)
```

---

# Features

- Continuous blockchain indexing
- Robust Cosmos transaction pagination
- Handles pagination edge cases
- Automatic recovery from RPC inconsistencies
- Missed reward windows are processed automatically
- Local SQLite storage
- Discord leaderboard integration
- Safe restart (progress stored in config)

---

# Database Schema

The service uses SQLite.

### `jobs`

Stores all detected jobs and results.

```
job_id
submission_txhash
submission_height
submission_timestamp
result_txhash
result_height
result_timestamp
creator
target_validator
fee
```

---

### `points`

Total accumulated validator reward points.

```
validator_address
pp
```

---

### `reward_windows`

Tracks processed reward windows to prevent double rewards.

```
window_start
window_end
total_jobs_completed
total_points_distributed
created_at
```

---

# Requirements

Python **3.10+** recommended.

Dependencies are managed using **venv**.

---

# Installation

Clone the repository.

```bash
git clone https://github.com/IAmScRay/republic_job_indexer
cd republic_testnet
```

Create a virtual environment.

```bash
python3 -m venv venv
```

Activate it.

Linux / macOS:

```bash
source venv/bin/activate
```

Windows:

```bash
venv\Scripts\activate
```

Install dependencies.

```bash
pip install -r requirements.txt
```

---

# Configuration

Create a config file from the example:

```bash
cp config.json.example config.json
```

Example configuration:

```json
{
    "api_url": "",
    "db_path": "",
    "discord_bot_token": "",
    "discord_channel_id": "",
    "page_limit": 100,
    "block_window_length": 100,
    "last_indexed_height": 1,
    "loop_sleep_seconds": 10
}
```

---

# Config Parameters

### `api_url`

LCD API endpoint for the Republic Testnet node.

Example:

```
http://localhost:1317
```

---

### `db_path`

Path to the SQLite database.

Example:

```
republic_jobs.db
```

---

### `discord_bot_token`

Discord bot token used to post leaderboard messages.

---

### `discord_channel_id`

Channel where leaderboard messages will be posted.

---

### `page_limit`

Maximum number of transactions returned per request.

Default:

```
100
```

---

### `block_window_length`

Block range used when querying transactions.

Example:

```
100 blocks
```

Smaller values improve reliability on high-traffic networks.

---

### `last_indexed_height`

Height from which indexing begins.

Automatically updated by the program.

---

### `loop_sleep_seconds`

Delay between indexing cycles.

Default:

```
10 seconds
```

---

# Running

Start the indexer:

```bash
python main.py
```

The service will run continuously until interrupted.

Stop with:

```
Ctrl + C
```

Progress will be saved automatically.

---

# Behavior

The service runs an infinite loop:

1. Fetch latest block height
2. Index new transactions
3. Update job database
4. Process any **missed reward windows**
5. Publish leaderboard updates
6. Sleep for configured interval

If the service is offline for several hours:

- All **missed 30-minute windows** will be processed automatically on restart.

---

# Reliability Features

The indexer includes protections for common Cosmos RPC issues:

- transaction pagination limits
- inconsistent pagination offsets
- RPC retry logic
- partial page detection
- window retry fallback

These safeguards ensure **no jobs are skipped** during indexing.
