import json
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT")
}
DATA_DIR = os.getenv("DATA_DIR")
BATCH_SIZE = 2000
TRACKER_FILE = "processed_files.log"

def parse_twitter_date(date_str):
    """
    Parses a Twitter-formatted date string into a datetime object.
    
    Args:
        date_str (str): The date string to parse.

    Returns:
        datetime: The parsed datetime object, or None if parsing fails.
    """
    
    if not date_str: return None
    try:
        # Standard Twitter format: "Sun Jun 17 17:00:31 +0000 2018"
        return datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')
    except Exception:
        return None

def get_processed_files():
    """
    Reads the log of already completed files.

    Returns:
        set: A set of filenames that have already been processed.
    """
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, 'r') as f:
            return set(f.read().splitlines())
    return set()

def mark_file_processed(filename):
    """
    Saves the filename so it is never processed again.

    Args:
        filename (str): The name of the file to mark as processed.
    """
    with open(TRACKER_FILE, 'a') as f:
        f.write(f"{filename}\n")

def upload_tweets():
    """
    Uploads tweet data from JSON files to the database.
    """
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    # Initialize Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMP,
            tweet_text TEXT,
            raw_data JSONB
        );
    """)
    conn.commit()

    # Find out what we actually need to do
    processed_files = get_processed_files()
    all_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".json")])
    files_to_process = [f for f in all_files if f not in processed_files]

    print(f"Found {len(all_files)} total files.")
    print(f"Skipping {len(processed_files)} already processed files.")
    print(f"{len(files_to_process)} files remaining for ingestion.\n")

    batch = []

    # Process only the files we haven't finished yet
    for idx, filename in enumerate(files_to_process, 1):
        file_path = os.path.join(DATA_DIR, filename)
        print(f"[{idx}/{len(files_to_process)}] Reading {filename}...", end="", flush=True)

        error_count = 0

        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line: continue

                try:
                    data = json.loads(line)

                    t_id = data.get('id')
                    t_text = data.get('full_text') or data.get('text')
                    t_date = parse_twitter_date(data.get('created_at'))

                    if t_id:
                        batch.append((t_id, t_date, t_text, json.dumps(data)))

                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO tweets (id, created_at, tweet_text, raw_data)
                            VALUES %s ON CONFLICT (id) DO NOTHING
                        """, batch)
                        conn.commit()
                        batch = []

                except json.JSONDecodeError:
                    error_count += 1
                except psycopg2.Error as db_err:
                    conn.rollback()
                    batch = []
                    error_count += 1
                except Exception as e:
                    error_count += 1

        # Push any leftover tweets at the end of the file
        if batch:
            try:
                execute_values(cur, """
                    INSERT INTO tweets (id, created_at, tweet_text, raw_data)
                    VALUES %s ON CONFLICT (id) DO NOTHING
                """, batch)
                conn.commit()
            except psycopg2.Error as db_err:
                conn.rollback()
                print(f"\nDB error on final flush for {filename}: {db_err}", flush=True)
                error_count += 1
            batch = []

        # 1. Mark file as done so we never repeat it
        mark_file_processed(filename)

        # 2. Get the real-time count
        cur.execute("SELECT count(*) FROM tweets;")
        total_db_count = cur.fetchone()[0]

        # 3. Print the update
        err_msg = f" | {error_count} bad lines" if error_count > 0 else ""
        print(f" Done! Total in DB: {total_db_count:,}{err_msg}")

    cur.close()
    conn.close()
    print("\nAll files imported successfully.")

if __name__ == "__main__":
    upload_tweets()
