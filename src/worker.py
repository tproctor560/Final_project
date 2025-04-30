import time
import json
import redis
import os
import logging
import re
import pandas as pd
from hotqueue import HotQueue
from datetime import datetime
from jobs import get_job_by_id, update_job_status
from api import load_data_from_csv

log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.DEBUG)

_redis_ip = os.environ.get('REDIS_HOST', 'redis-db')
_redis_port = int(os.environ.get('REDIS_PORT', 6379))

rd = redis.Redis(host=_redis_ip, port=_redis_port, db=0)  # HGNC data
q = HotQueue("queue", host=_redis_ip, port=_redis_port, db=1)  # Queue
jdb = redis.Redis(host=_redis_ip, port=_redis_port, db=2)  # Job metadata
results_db = redis.Redis(host=_redis_ip, port=_redis_port, db=3, decode_responses=True)  # Results

def run_worker_job_logic(job_id: str) -> None:
    """
    Core logic for processing a job. Can be tested independently of HotQueue.
    """
    logging.info(f"Worker received job_id: {job_id}")
    logging.info(f"Worker picked up job {job_id} from queue.")
    try:
        job = get_job_by_id(job_id)

        if not job:
            logging.warning(f"Job {job_id} not found in Redis.")
            return

        logging.info(f"Processing job {job_id}...")
        update_job_status(job_id, "in progress")

        time.sleep(1)  # Optional: simulate short work

        # Extract date range
        start_date_str = job.get("start")
        end_date_str = job.get("end")

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError:
            logging.error(f"Invalid date format in job {job_id}. Marking as failed.")
            update_job_status(job_id, "failed")
            return

        # Load HGNC data
        raw_data = json.loads(rd.get("hgnc_data") or "{}")
        
        # TODO: Convert Raw_data to df when loading or upload df when loading
        gene_list = raw_data.get("response", {}).get("docs", [])
        logging.info(f"Filtering genes for job {job_id} between {start_date_str} and {end_date_str}.")
        filtered_genes = []
        for gene in gene_list:
            date_str = gene.get("date_approved_reserved")
            try:
                if date_str:
                    gene_date = datetime.strptime(date_str, "%Y-%m-%d")
                    if start_date <= gene_date <= end_date:
                        filtered_genes.append(gene)
            except ValueError:
                continue  # Skip malformed dates

        locus_counts = {}
        for gene in filtered_genes:
            locus_type = gene.get("locus_type", "Unknown")
            locus_counts[locus_type] = locus_counts.get(locus_type, 0) + 1

        result = {
            "job_id": job_id,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "total_genes_counted": len(filtered_genes),
            "locus_type_counts": locus_counts
        }

        results_db.set(job_id, json.dumps(result))
        logging.info(f"Result saved for job {job_id}. Total genes counted: {len(filtered_genes)}")
        update_job_status(job_id, "complete")
        logging.info(f"Job {job_id} marked as complete.")

    except Exception as e:
        logging.error(f"Exception while processing job {job_id}: {str(e)}")
        update_job_status(job_id, "failed")


def find_injuries_from_list(list_of_plays):
    try:
        df_list_of_plays = pd.DataFrame(list_of_plays)
        INJURY_LIST = [
                r'injur(y|ed|ies)',
                r'carted off',
                r'helped off',
                r'assisted off',
                r'left the (game|field)',
                r'medical attention',
                r'trainers',
                r'concussion',
                r'protocol',
                r'did not return',
                r'evaluated for',
                r'medical tent',
                r'questionable to return',
                r'doubtful to return',
                r'out for the game',
                r'is down',
                r'stays down',
                r'remained down'
            ]
        
        injury_pattern = "|".join(INJURY_LIST)
        
        df_list_of_plays['Contains_Injury'] = df_list_of_plays['Description'].str.contains(
            injury_pattern,
            case=False,
            regex=True
            na=False
        )
        
        injury_plays = df_list_of_plays[df_list_of_plays['Contains_Injury']]
        non_injury_plays = df_list_of_plays[df_list_of_plays['Contains_Injury'] == False]
        
        return injury_plays, non_injury_plays
        
    except Exception as e:
        logging.warning("Error while processing data for injuries")
        return False, False
    

@q.worker
def do_work(job_id):
    """
    HotQueue entrypoint for processing jobs.
    """
    
    
    
    run_worker_job_logic(job_id)


if __name__ == "__main__":
    load_data_from_csv()
    do_work()
