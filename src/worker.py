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

rd = redis.Redis(host=_redis_ip, port=_redis_port, db=0)  # raw data
q = HotQueue("queue", host=_redis_ip, port=_redis_port, db=1)  # hot queue
jdb = redis.Redis(host=_redis_ip, port=_redis_port, db=2)  # job DB
results_db = redis.Redis(host=_redis_ip, port=_redis_port, db=3, decode_responses=True)  # results

def run_worker_job_logic(job_id: str) -> None:
    logging.info(f"Worker picked up job {job_id} from queue.")

    try:
        job = get_job_by_id(job_id)
        if not job:
            logging.warning(f"Job {job_id} not found.")
            return

        update_job_status(job_id, "in progress")

        start_date_str = job.get("start")
        end_date_str = job.get("end")

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError:
            logging.error(f"Invalid date format for job {job_id}.")
            update_job_status(job_id, "failed")
            return

        raw_data = rd.get("hgnc_data")
        if not raw_data:
            logging.error("No NFL data found in Redis.")
            update_job_status(job_id, "failed")
            return

        play_list = json.loads(raw_data)

        # Filter: plays that mention "injured" within date range
        filtered_plays = []
        for play in play_list:
            try:
                play_date = datetime.strptime(play.get("GameDate", "1900-01-01"), "%Y-%m-%d")
                if start_date <= play_date <= end_date and "injured" in play.get("Description", "").lower():
                    filtered_plays.append(play)
            except Exception:
                continue  # Skip bad dates

        # Count injuries by (Formation, PlayType)
        combo_counts = {}
        for play in filtered_plays:
            formation = play.get("Formation", "Unknown")
            play_type = play.get("PlayType", "Unknown")
            combo = f"{formation} - {play_type}"
            combo_counts[combo] = combo_counts.get(combo, 0) + 1

        result = {
            "job_id": job_id,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "total_injured_plays": len(filtered_plays),
            "injured_playtype_formation_counts": combo_counts
        }

        results_db.set(job_id, json.dumps(result))
        update_job_status(job_id, "complete")
        logging.info(f"Job {job_id} completed. Injured plays: {len(filtered_plays)}.")

    except Exception as e:
        logging.error(f"Error processing job {job_id}: {e}")
        update_job_status(job_id, "failed")

@q.worker
def do_work(job_id):
    run_worker_job_logic(job_id)

if __name__ == "__main__":
    do_work()
