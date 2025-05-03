import time
import json
import redis
import os
import logging
from hotqueue import HotQueue
from datetime import datetime
from jobs import get_job_by_id, update_job_status

# Setup
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.DEBUG)

_redis_ip = os.environ.get('REDIS_HOST', 'redis-db')
_redis_port = int(os.environ.get('REDIS_PORT', 6379))

# Redis connections
try:
    rd = redis.Redis(host=_redis_ip, port=_redis_port, db=0)
    q = HotQueue("queue", host=_redis_ip, port=_redis_port, db=1)
    jdb = redis.Redis(host=_redis_ip, port=_redis_port, db=2)
    results_db = redis.Redis(host=_redis_ip, port=_redis_port, db=3, decode_responses=True)
    logging.info("Worker connected to Redis databases.")
except Exception as e:
    logging.error(f"Failed to connect to Redis: {e}")
    raise

def run_worker_job_logic(job_id: str) -> None:
    logging.info(f"Worker picked up job {job_id} from queue.")
    try:
        job = get_job_by_id(job_id)
        if not job:
            logging.warning(f"Job {job_id} not found in DB.")
            return

        update_job_status(job_id, "in progress")

        start_date = datetime.strptime(job.get("start"), "%Y-%m-%d")
        end_date = datetime.strptime(job.get("end"), "%Y-%m-%d")

        raw_data = rd.get("nfl_data")
        if not raw_data:
            logging.error("No NFL data found in Redis.")
            update_job_status(job_id, "failed")
            return

        play_list = json.loads(raw_data)
        injury_combo_counts = {}

        for play in play_list:
            try:
                play_date = datetime.strptime(play.get("GameDate", "1900-01-01"), "%Y-%m-%d")
                if not (start_date <= play_date <= end_date):
                    continue

                description = play.get("Description", "").lower()
                formation = play.get("Formation", "Unknown")
                play_type = play.get("PlayType", "").upper()

                if play_type == "RUSH":
                    direction = play.get("RushDirection", "Unknown")
                elif play_type == "PASS":
                    direction = play.get("PassType", "Unknown")
                else:
                    continue

                key = f"Formation: {formation}; PlayType: {play_type}; Direction: {direction}"
                if key not in injury_combo_counts:
                    injury_combo_counts[key] = {
                        "injury_plays": 0,
                        "total_plays": 0,
                        "injury_percentage": 0.0
                    }

                injury_combo_counts[key]["total_plays"] += 1
                if "injured" in description:
                    injury_combo_counts[key]["injury_plays"] += 1

                total = injury_combo_counts[key]["total_plays"]
                injuries = injury_combo_counts[key]["injury_plays"]
                injury_combo_counts[key]["injury_percentage"] = round((injuries / total) * 100, 2) if total > 0 else 0.0

            except Exception as e:
                logging.warning(f"Error parsing play: {e}")
                continue

        result = {
            "job_id": job_id,
            "start_date": job.get("start"),
            "end_date": job.get("end"),
            "injury_combo_counts": injury_combo_counts
        }

        results_db.set(job_id, json.dumps(result))
        update_job_status(job_id, "complete")
        logging.info(f"Job {job_id} completed and result stored in DB.")

    except Exception as e:
        logging.error(f"Worker failed processing job {job_id}: {e}")
        update_job_status(job_id, "failed")

@q.worker
def do_work(job_id):
    logging.info(f"Dequeued job from HotQueue: {job_id}")
    run_worker_job_logic(job_id)

if __name__ == "__main__":
    logging.info("Worker is listening for jobs...")
    do_work()
