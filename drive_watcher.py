#!/usr/bin/env python3
"""
Google Drive Folder Watcher + Telegram Notifier (rclone edition)
-----------------------------------------------------------------
Uses rclone instead of Google API — no Google Cloud Console needed.

Prerequisites:
  1. Install rclone:        https://rclone.org/install/
  2. Configure Drive once:  rclone config   (follow prompts, name it "gdrive")
  3. pip install requests

Usage:
  python drive_watcher.py
"""

import os
import sys
import subprocess
import atexit
import logging
import json
import requests
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

load_dotenv()

# ──────────────────────────────────────────────
# CONFIGURATION — fill these in before running
# ──────────────────────────────────────────────

# The rclone remote name you chose during `rclone config` (default: "gdrive")
RCLONE_REMOTE = os.getenv("RCLONE_REMOTE")

# The Google Drive folder — two options:
# Option A — folder inside your own Drive (use the path as it appears in Drive):
#   DRIVE_FOLDER = "gdrive:Physics Wallah/Lectures"
# Option B — a shared folder URL (paste the full link):
#   DRIVE_FOLDER = "https://drive.google.com/drive/folders/FOLDER_ID_HERE"
DRIVE_FOLDER = os.getenv("DRIVE_FOLDER")

# Telegram bot token (from @BotFather) and your chat/user ID
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID").split(",");
# Where to save downloaded files locally
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR"))

# Nhost DB URL from .env file
NHOST_DB_URL = os.environ.get("NHOST_DB_URL")

# State file — tracks which files have already been downloaded
STATE_FILE = Path("./state.json")
LOG_FILE = "./Scrapper.log"

# ──────────────────────────────────────────────
# Setup logging to match pdf_sync.py

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def resolve_folder(folder: str):
    """
    Returns (rclone_path, folder_id).
    If a Drive URL is given, extract the folder ID for --drive-root-folder-id.
    Otherwise treat as a normal rclone path like 'gdrive:Some/Folder'.
    """
    if folder.startswith("http"):
        import re
        match = re.search(r"/folders/([a-zA-Z0-9_-]+)", folder)
        if not match:
            raise ValueError(f"Could not extract folder ID from URL: {folder}")
        return None, match.group(1)
    return folder, None


def list_remote_files(rclone_path, folder_id) -> list:
    """Use rclone lsjson --recursive to list ALL files including inside subfolders."""
    if folder_id:
        cmd = [
            "rclone", "lsjson", f"{RCLONE_REMOTE}:",
            "--drive-root-folder-id", folder_id,
            "--files-only",
            "--recursive",
        ]
    else:
        cmd = ["rclone", "lsjson", rclone_path, "--files-only", "--recursive"]

    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        raise RuntimeError(f"rclone lsjson failed:\n{result.stderr.strip()}")

    return json.loads(result.stdout)


def download_file(file_path: str, rclone_path, folder_id, dest_dir: Path):
    """
    Download a single file using rclone copy.
    file_path is the relative path e.g. 'Chaitanya Sir /Lecture1.pdf'
    rclone copy preserves the subfolder structure inside dest_dir.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    if folder_id:
        # For shared folders via folder ID, source is remote:file_path
        src = f"{RCLONE_REMOTE}:{file_path}"
        cmd = [
            "rclone", "copy", src, str(dest_dir / Path(file_path).parent),
            "--drive-root-folder-id", folder_id,
        ]
    else:
        src = f"{rclone_path}/{file_path}"
        cmd = ["rclone", "copy", src, str(dest_dir / Path(file_path).parent)]

    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        raise RuntimeError(f"rclone copy failed:\n{result.stderr.strip()}")


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except json.JSONDecodeError:
            logger.warning("⚠️ Warning: state.json is corrupted. Starting fresh locally.")
    return {"downloaded": []}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def sync_state(conn, local_state: dict) -> set:
    """
    Syncs local state.json with Postgres.
    Merges both lists, saves locally, and inserts missing ones into Postgres.
    Returns the unified set of file keys.
    """
    db_keys = set()
    if conn:
        try:
            db_keys = get_downloaded_files(conn)
        except Exception as e:
            logger.warning(f"⚠️ Warning: Failed to fetch files from DB during sync: {e}")

    local_keys = set(local_state.get("downloaded", []))
    
    # Merge both local and remote keys
    unified_keys = local_keys.union(db_keys)

    # 1. Update local state json if any new records came from DB
    if len(unified_keys) > len(local_keys):
        local_state["downloaded"] = list(unified_keys)
        save_state(local_state)

    # 2. Update DB with any records that were local but not in DB
    if conn:
        missing_in_db = local_keys - db_keys
        if missing_in_db:
            logger.info(f"🔄 Syncing {len(missing_in_db)} local records to Database...")
            try:
                with conn.cursor() as cur:
                    for key in missing_in_db:
                        cur.execute(
                            "INSERT INTO downloaded_files (file_key) VALUES (%s) ON CONFLICT (file_key) DO NOTHING;",
                            (key,)
                        )
                conn.commit()
            except Exception as e:
                logger.error(f"⚠️ Warning: Failed to push local files to DB: {e}")

    return unified_keys


def init_db(conn):
    """Create the downloaded_files table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS downloaded_files (
                id SERIAL PRIMARY KEY,
                file_key TEXT UNIQUE NOT NULL,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    conn.commit()


def get_downloaded_files(conn) -> set:
    """Fetch the set of file keys that have been downloaded."""
    with conn.cursor() as cur:
        cur.execute("SELECT file_key FROM downloaded_files;")
        rows = cur.fetchall()
        return {row[0] for row in rows}


def mark_file_downloaded(conn, file_key: str):
    """Mark a file as downloaded in the database."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO downloaded_files (file_key) VALUES (%s) ON CONFLICT (file_key) DO NOTHING;",
            (file_key,)
        )
    conn.commit()


def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    for id in TELEGRAM_CHAT_ID:
        resp = requests.post(url, json={
            "chat_id": id,
            "text": message,
            "parse_mode": "Markdown",
        }, timeout=10)
        if not resp.ok:
            logger.error(f"  ⚠️  Telegram error: {resp.status_code} {resp.text}")


def check_rclone():
    result = subprocess.run(["rclone", "version"], capture_output=True)
    if result.returncode != 0:
        logger.error("❌ rclone not found. Install it from https://rclone.org/install/")
        exit(1)


def main():
    logger.info(f"🔍 Drive Watcher — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("─" * 50)

    if "YOUR_" in DRIVE_FOLDER or "YOUR_" in TELEGRAM_BOT_TOKEN:
        logger.error("❌ Please fill in DRIVE_FOLDER, TELEGRAM_BOT_TOKEN, and TELEGRAM_CHAT_ID.")
        return

    if not NHOST_DB_URL:
        logger.error("❌ Please set NHOST_DB_URL in your .env file.")
        return

    check_rclone()

    rclone_path, folder_id = resolve_folder(DRIVE_FOLDER)
    logger.info(f"📁 Folder ID: {folder_id or rclone_path}")

    # List all files recursively
    logger.info("📋 Listing files in remote folder (recursive)...")
    try:
        files = list_remote_files(rclone_path, folder_id)
    except RuntimeError as e:
        print(f"❌ {e}")
        return
    # Connect to database and init
    logger.info("🔌 Attempting to connect to Nhost Database...")
    conn = None
    if NHOST_DB_URL:
        try:
            conn = psycopg2.connect(NHOST_DB_URL)
            init_db(conn)
            logger.info("✅ Connected to Database.")
        except Exception as e:
            logger.error(f"⚠️ Warning: Database connection failed. Falling back to local state. Error: {e}")
            conn = None

    # Load local state and sync with DB
    local_state = load_state()
    logger.info("🔄 Syncing local and remote states...")
    known = sync_state(conn, local_state)

    # Detect new files by comparing Drive file IDs against saved state
    new_files = [f for f in files if f.get("ID", f["Path"]) not in known]
    logger.info(f"   {len(new_files)} new file(s) to download")

    if not new_files:
        logger.info("✅ Nothing new. All done!")
        if conn:
            conn.close()
        return

    downloaded_names = []
    for f in new_files:
        file_path = f["Path"]                    # e.g. "Chaitanya Sir /Lecture1.pdf"
        file_key  = f.get("ID", file_path)       # prefer stable Drive ID
        size      = f.get("Size", 0)
        size_str  = f"{size / 1024:.1f} KB" if size else "unknown size"

        logger.info(f"\n  ⬇️  Downloading: {file_path} ({size_str})")
        try:
            download_file(file_path, rclone_path, folder_id, DOWNLOAD_DIR)
            print(f"     ✅ Saved to {DOWNLOAD_DIR / file_path}")
            
            # Save strictly to local file first
            if file_key not in local_state["downloaded"]:
                local_state["downloaded"].append(file_key)
                save_state(local_state)

            # Optimistically save to database
            if conn:
                try:
                    mark_file_downloaded(conn, file_key)
                except Exception as e:
                    logger.error(f"     ⚠️ DB Insert failed (local saved): {e}")

            downloaded_names.append(file_path)
        except RuntimeError as e:
            logger.error(f"     ❌ Failed: {e}")

    if conn:
        try:
            conn.close()
        except:
            pass

    if downloaded_names:
        logger.info("\n📤 Sending Telegram notification...")
        
        chunks = []
        current_chunk = []
        current_len = 0
        
        for name in downloaded_names:
            line = f"  • `{name}`"
            line_len = len(line) + 1  # for newline
            if current_len + line_len > 3500 and current_chunk:
                chunks.append(current_chunk)
                current_chunk = [line]
                current_len = line_len
            else:
                current_chunk.append(line)
                current_len += line_len
                
        if current_chunk:
            chunks.append(current_chunk)
            
        for i, chunk in enumerate(chunks):
            file_list = "\n".join(chunk)
            if len(chunks) == 1:
                header = f"📥 *Drive Watcher* — {len(downloaded_names)} new file(s) downloaded!"
            else:
                header = f"📥 *Drive Watcher* — {len(downloaded_names)} new file(s) downloaded! (Part {i+1}/{len(chunks)})"
                
            msg = (
                f"{header}\n\n"
                f"{file_list}\n\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_telegram(msg)
            
        logger.info("   ✅ Notification sent!")

    logger.info(f"\n✅ Done! Downloaded {len(downloaded_names)} file(s).\n")

def cleanup():
    """Terminate background processes on exit."""
    global _loki_process
    if _loki_process is not None:
        logger.info("Terminating background Loki logger...")
        try:
             # Send termination signal via log file
             logger.info("LOKI_LOGGER_TERMINATE")
             _loki_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _loki_process.kill()
        except Exception as e:
            logger.error(f"Error terminating Loki logger: {e}")
        logger.info("Loki logger terminated.")

# Register the cleanup function to run automatically when the script exits
atexit.register(cleanup)

if __name__ == "__main__":
    # Global reference to the background loki logger process
    _loki_process = None

    def init_loki_logger(log_file):
        global _loki_process
        
        # Calculate where to start reading the log file (so we don't resend old logs)
        start_pos = 0
        if os.path.exists(log_file):
            start_pos = os.path.getsize(log_file)
        
        loki_url = os.environ.get("LOKI_URL")
        if loki_url:
            # Assumes loki_logger.py is in the same directory as this script
            base_dir = os.path.dirname(os.path.abspath(__file__))
            loki_script_path = os.path.join(base_dir, "loki_logger.py")
        
            if os.path.exists(loki_script_path):
                logger.info(f"Starting background Loki logger streaming to {loki_url}")
                try:
                    _loki_process = subprocess.Popen(
                        [sys.executable, loki_script_path, log_file, str(start_pos)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True # Runs independently
                    )
                except Exception as e:
                    logger.error(f"Failed to start Loki logger subprocess: {e}")
            else:
                logger.warning("loki_logger.py not found. Loki logging will not be available.")

    # Call this to start the logger (pass the path to your log file)
    init_loki_logger(LOG_FILE) # Assuming LOG_FILE = "./compressor.log" from Step 2
    main()