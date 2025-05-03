import os
import json
import redis
import logging
from hotqueue import HotQueue
from datetime import datetime
from dateutil import parser
from jobs import get_job_by_id, update_job_status

# Setup logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger(__name__)

# Redis configuration
_redis_ip = os.environ.get('REDIS_HOST', 'redis-db')
_redis_port = int(os.environ.get('REDIS_PORT', 6379))

# Redis connections
try:
    rd = redis.Redis(host=_redis_ip, port=_redis_port, db=0)  # Raw NFL data
    q = HotQueue("queue", host=_redis_ip, port=_redis_port, db=1)  # Job queue
    jdb = redis.Redis(host=_redis_ip, port=_redis_port, db=2)  # Job metadata
    results_db = redis.Redis(host=_redis_ip, port=_redis_port, db=3, decode_responses=True)  # Analysis results
    logger.info("Worker connected to Redis databases.")
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    raise

def run_worker_job_logic(job_id: str) -> None:
    logger.info(f"Worker picked up job {job_id} from queue.")
    try:
        job = get_job_by_id(job_id)
        if not job:
            logger.warning(f"Job {job_id} not found in job DB.")
            return

        update_job_status(job_id, "in progress")
        logger.debug(f"Set job {job_id} to in progress.")

        # Parse date range from job
        start_date = datetime.strptime(job.get("start"), "%Y-%m-%d")
        end_date = datetime.strptime(job.get("end"), "%Y-%m-%d")

        # Load NFL data from Redis
        raw_data = rd.get("nfl_data")
        if not raw_data:
            logger.error("No NFL data found in Redis DB 0 under key 'nfl_data'.")
            update_job_status(job_id, "failed")
            return

        play_list = json.loads(raw_data)
        injury_combo_counts = {}

        for idx, play in enumerate(play_list):
            try:
                game_date_str = play.get("GameDate", "1900-01-01")
                play_date = parser.parse(game_date_str)

                if not (start_date <= play_date <= end_date):
                    continue

                description = play.get("Description", "").lower()
                formation = play.get("Formation", "Unknown")
                play_type = play.get("PlayType", "").upper()

                if play_type not in ["RUSH", "PASS"]:
                    continue

                direction = play.get("RushDirection", "Unknown") if play_type == "RUSH" else play.get("PassType", "Unknown")
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
                injury_combo_counts[key]["injury_percentage"] = round((injuries / total) * 100, 2)

            except Exception as e:
                logger.warning(f"Error parsing play #{idx}: {e}")
                continue

        result = {
            "job_id": job_id,
            "start_date": job.get("start"),
            "end_date": job.get("end"),
            "injury_combo_counts": injury_combo_counts
        }

        results_db.set(job_id, json.dumps(result))
        logger.debug(f"Stored result for job {job_id} in Redis DB 3.")
        update_job_status(job_id, "complete")
        logger.info(f"Job {job_id} completed successfully.")

    except Exception as e:
        logger.error(f"Worker failed processing job {job_id}: {e}")
        update_job_status(job_id, "failed")

@q.worker
def do_work(job_id):
    run_worker_job_logic(job_id)

if __name__ == "__main__":
    do_work()
