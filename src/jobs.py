import os
import json
import uuid
import redis
import logging
from hotqueue import HotQueue

log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.DEBUG)

_redis_ip = os.environ.get('REDIS_HOST', 'redis-db')
_redis_port = int(os.environ.get('REDIS_PORT', 6379))


rd = redis.Redis(host=_redis_ip, port=_redis_port, db=0)
q = HotQueue("queue", host=_redis_ip, port=_redis_port, db=1)
jdb = redis.Redis(host=_redis_ip, port=_redis_port, db=2)
results_db = redis.Redis(host=_redis_ip, port=_redis_port, db=3, decode_responses=True)

def _generate_jid():
    """
    Generate a pseudo-random identifier for a job.

    Args: none
    
    Returns:
        str: a unique job identifier as a UUID string
    """
    logging.debug("Generating new job ID.")
    return str(uuid.uuid4())

def _instantiate_job(jid, status, start, end):
    """
    Create the job object description as a python dictionary. Requires the job id,
    status, start and end parameters.

    Args:
        jid (str): Unique job ID.
        status (str): Current status of the job (e.g., "submitted").
        start (str): Start date of the range (YYYY-MM-DD).
        end (str): End date of the range (YYYY-MM-DD).

    Returns:
        dict: A dictionary representing the job object
    """
    logging.debug(f"Instantiating job {jid} with status '{status}' from {start} to {end}.")
    return {'id': jid,
            'status': status,
            'start': start,
            'end': end }

def _save_job(jid, job_dict):
    """Save a job object in the Redis database.
    
    Args:
        jid (str): The job ID.
        job_dict (dict): The job data to be stored.

    Returns:
        None
    """
    logging.info(f"Saving job {jid} to Redis DB.")
    jdb.set(jid, json.dumps(job_dict))
    return

def _queue_job(jid):
    """Add a job to the redis queue.
    
    Args:
        jid (str): The job ID to enqueue.

    Returns:
        None
    
    """
    logging.info(f"Queueing job {jid} for processing.")
    q.put(jid)
    return

def add_job(start, end, method, status="submitted"):
    """Add a job to the redis queue.
    
    Args:
        start (str): Start date of the range (YYYY-MM-DD).
        end (str): End date of the range (YYYY-MM-DD).
        status (str, optional): Initial job status. Default is "submitted".

    Returns:
        dict: The job dictionary that was created and queued.
    """
    logging.info(f"Creating new job for range {start} to {end} with status '{status}'.")
    jid = _generate_jid()
    job_dict = _instantiate_job(jid, status, start, end)
    _save_job(jid, job_dict)
    _queue_job(jid)
    return job_dict

def get_job_by_id(jid):
    """Return job dictionary given jid
    
    Args:
        jid (str): The job ID to retrieve.

    Returns:
        dict: The job dictionary retrieved from Redis.
    """
    logging.debug(f"Fetching job {jid} from Redis DB.")
    return json.loads(jdb.get(jid))

def update_job_status(jid, status):
    """Update the status of job with job id `jid` to status `status`.
    
    Args:
        jid (str): The job ID to update.
        status (str): The new status to assign to the job.

    Returns:
        None
    
    """
    logging.info(f"Updating job {jid} status to '{status}'.")
    job_dict = get_job_by_id(jid)
    if job_dict:
        job_dict['status'] = status
        _save_job(jid, job_dict)
    else:
        logging.error(f"Failed to update job status. Job {jid} not found.")
        raise Exception("Job not found")
