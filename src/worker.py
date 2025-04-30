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
        method = job.get("method")

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError:
            logging.error(f"Invalid date format in job {job_id}. Marking as failed.")
            update_job_status(job_id, "failed")
            return

        # Load Play data
        raw_data = json.loads(rd.get("hgnc_data") or "{}")
        
        plays_list = raw_data.get("response", {}).get("docs", [])
        logging.info(f"Filtering plays for job {job_id} between {start_date_str} and {end_date_str}.")
        filtered_genes = []
        
        list_of_plays = plays_list[plays_list['GameDate'].between(start_date, end_date, inclusive='both')]
        
        #TODO: Move functions over to startup
        modified_list_of_plays = find_injuries_from_list(gene_list)

        if method.startswith("plays/"):
            if method != "plays/":
                playID = method.split("/")[1]
                try:
                    logging.info(f"Request recieved for certain playID {playID}")
                    specific_play = list_of_plays[list_of_plays["play_id"] == playID].copy()
                    result = {
                        "job_id": job_id,
                        "start_date": start_date_str,
                        "end_date": end_date_str,
                        results: {
                            "specific play": specific_play.to_json(orient='records', lines=True),
                        }
                    }
                    
                    results_db.set(job_id, json.dumps(result))
                    logging.info(f"Result saved for job {job_id}")
                    update_job_status(job_id, "complete")
                    logging.info(f"Job {job_id} marked as complete.")
                except Exception as e:
                    logging.warning(f"Error: {playID} returns error")
            if method == "plays/":
                try:
                    stat_results = compute_statistics(list_of_plays)
                    result = {
                        "job_id": job_id,
                        "start_date": start_date_str,
                        "end_date": end_date_str,
                        results: {
                            "specific play": stat_results.to_json(orient='records', lines=True),
                        }
                    }
                    results_db.set(job_id, json.dumps(result))
                    logging.info(f"Result saved for job {job_id}")
                    update_job_status(job_id, "complete")
                    logging.info(f"Job {job_id} marked as complete.")
                    
        #TODO: Add functionality for injuries

    except Exception as e:
        logging.error(f"Exception while processing job {job_id}: {str(e)}")
        update_job_status(job_id, "failed")

def find_injuries_from_list(list_of_plays):
    # TODO: Write a descriptor, mention that the list was made using claude 3.7 (Prompt: Can you write a list of injuries that can identify if a play ended in injury based on the following file head:)
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
        
        return df_list_of_plays
        
    except Exception as e:
        logging.warning("Error while processing data for injuries")
        return False, False
    
def compute_statistics(list_of_plays, injuries=False):
    #TODO: write function description, written by Claude 3.7, with the prompt: Write a function that takes in the df head at the top and computes statistics about injuries, for example how many passes or rushes ended in injuries?
    injury_plays = df[df['ContainsInjury']]
    total_plays = len(df)
    total_injury_plays = len(injury_plays)
    injury_percentage = (total_injury_plays / total_plays * 100) if total_plays > 0 else 0
    # ---- Rush Play Statistics ----
    rush_plays = df[df['IsRush'] == 1]
    rush_count = len(rush_plays)
    rush_injury_plays = rush_plays[rush_plays['ContainsInjury']]
    rush_injury_count = len(rush_injury_plays)
    rush_injury_pct = (rush_injury_count / rush_count * 100) if rush_count > 0 else 0
    
    # Rush direction analysis (if available)
    rush_direction_stats = {}
    if 'RushDirection' in df.columns:
        for direction in rush_plays['RushDirection'].dropna().unique():
            dir_plays = rush_plays[rush_plays['RushDirection'] == direction]
            dir_count = len(dir_plays)
            dir_injury_count = len(dir_plays[dir_plays['ContainsInjury']])
            dir_pct = (dir_injury_count / dir_count * 100) if dir_count > 0 else 0
            
            rush_direction_stats[direction] = {
                'total_plays': dir_count,
                'injury_plays': dir_injury_count,
                'injury_percentage': round(dir_pct, 2)
            }
    
    # ---- Pass Play Statistics ----
    pass_plays = df[df['IsPass'] == 1]
    pass_count = len(pass_plays)
    pass_injury_plays = pass_plays[pass_plays['ContainsInjury']]
    pass_injury_count = len(pass_injury_plays)
    pass_injury_pct = (pass_injury_count / pass_count * 100) if pass_count > 0 else 0
    
    # Complete vs incomplete passes
    complete_passes = pass_plays[pass_plays['IsIncomplete'] == 0]
    complete_count = len(complete_passes)
    complete_injury_count = len(complete_passes[complete_passes['ContainsInjury']])
    complete_pct = (complete_injury_count / complete_count * 100) if complete_count > 0 else 0
    
    incomplete_passes = pass_plays[pass_plays['IsIncomplete'] == 1]
    incomplete_count = len(incomplete_passes)
    incomplete_injury_count = len(incomplete_passes[incomplete_passes['ContainsInjury']])
    incomplete_pct = (incomplete_injury_count / incomplete_count * 100) if incomplete_count > 0 else 0
    
    # ---- Down Analysis ----
    down_stats = {}
    for down in sorted([d for d in df['Down'].unique() if pd.notna(d)]):
        down_plays = df[df['Down'] == down]
        down_count = len(down_plays)
        down_injury_count = len(down_plays[down_plays['ContainsInjury']])
        down_pct = (down_injury_count / down_count * 100) if down_count > 0 else 0
        
        down_stats[int(down)] = {
            'total_plays': down_count,
            'injury_plays': down_injury_count,
            'injury_percentage': round(down_pct, 2)
        }
    
    # ---- Quarter Analysis ----
    quarter_stats = {}
    for quarter in sorted([q for q in df['Quarter'].unique() if pd.notna(q)]):
        quarter_plays = df[df['Quarter'] == quarter]
        quarter_count = len(quarter_plays)
        quarter_injury_count = len(quarter_plays[quarter_plays['ContainsInjury']])
        quarter_pct = (quarter_injury_count / quarter_count * 100) if quarter_count > 0 else 0
        
        quarter_stats[int(quarter)] = {
            'total_plays': quarter_count,
            'injury_plays': quarter_injury_count,
            'injury_percentage': round(quarter_pct, 2)
        }
    
    # Compile results
    results = {
        'date_range': {
            'oldest_date': oldest_date,
            'newest_date': newest_date
        },
        'overall_stats': {
            'total_plays': total_plays,
            'injury_plays': total_injury_plays,
            'injury_percentage': round(injury_percentage, 2)
        },
        'rush_analysis': {
            'total_plays': rush_count,
            'injury_plays': rush_injury_count,
            'injury_percentage': round(rush_injury_pct, 2),
            'by_direction': rush_direction_stats
        },
        'pass_analysis': {
            'total_plays': pass_count,
            'injury_plays': pass_injury_count,
            'injury_percentage': round(pass_injury_pct, 2),
            'complete_passes': {
                'total': complete_count,
                'injuries': complete_injury_count,
                'injury_percentage': round(complete_pct, 2)
            },
            'incomplete_passes': {
                'total': incomplete_count,
                'injuries': incomplete_injury_count,
                'injury_percentage': round(incomplete_pct, 2)
            }
        },
        'by_down': down_stats,
        'by_quarter': quarter_stats
    }
    
    return results

    

@q.worker
def do_work(job_id):
    """
    HotQueue entrypoint for processing jobs.
    """
    
    
    
    run_worker_job_logic(job_id)


if __name__ == "__main__":
    load_data_from_csv()
    do_work()
