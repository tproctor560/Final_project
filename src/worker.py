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

        raw_data = rd.get("nfl_data")
        if not raw_data:
            logging.error("No NFL data found in Redis.")
            update_job_status(job_id, "failed")
            return

        play_list = json.loads(raw_data)
        filtered = [p for p in play_list if "injured" in p.get("Description", "").lower()]

        rush_counts, pass_counts = {}, {}
        rush_total_all, pass_total_all = {}, {}

        for play in play_list:
            formation = play.get("Formation", "Unknown")
            play_type = play.get("PlayType", "").lower()
            if play_type == "rush":
                rush_dir = play.get("RushDirection", "Unknown")
                combo = f"{formation} - {rush_dir}"
                rush_total_all[combo] = rush_total_all.get(combo, 0) + 1
            elif play_type == "pass":
                pass_type = play.get("PassType", "Unknown")
                combo = f"{formation} - {pass_type}"
                pass_total_all[combo] = pass_total_all.get(combo, 0) + 1

        for play in filtered:
            try:
                play_date = datetime.strptime(play.get("GameDate", "1900-01-01"), "%Y-%m-%d")
                if not (start_date <= play_date <= end_date):
                    continue

                formation = play.get("Formation", "Unknown")
                play_type = play.get("PlayType", "").lower()

                if play_type == "rush":
                    rush_dir = play.get("RushDirection", "Unknown")
                    combo = f"{formation} - {rush_dir}"
                    rush_counts[combo] = rush_counts.get(combo, 0) + 1

                elif play_type == "pass":
                    pass_type = play.get("PassType", "Unknown")
                    combo = f"{formation} - {pass_type}"
                    pass_counts[combo] = pass_counts.get(combo, 0) + 1
            except Exception:
                continue

        rush_percent = {
            k: (rush_counts.get(k, 0) / v * 100) if v else 0
            for k, v in rush_total_all.items()
        }

        pass_percent = {
            k: (pass_counts.get(k, 0) / v * 100) if v else 0
            for k, v in pass_total_all.items()
        }

        result = {
            "job_id": job_id,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "rush_injury_percentages": rush_percent,
            "pass_injury_percentages": pass_percent
        }

        results_db.set(job_id, json.dumps(result))
        update_job_status(job_id, "complete")
        logging.info(f"Job {job_id} completed and result saved.")

    except Exception as e:
        logging.error(f"Error processing job {job_id}: {e}")
        update_job_status(job_id, "failed")

@q.worker
def do_work(job_id):
    run_worker_job_logic(job_id)

if __name__ == "__main__":
    do_work()
