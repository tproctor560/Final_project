
import requests
import uuid
import math
import logging
from flask import Flask, request, jsonify
import json
import os
import redis 
from datetime import datetime
from jobs import add_job
from jobs import get_job_by_id
from jobs import jdb
from jobs import results_db


app = Flask(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.DEBUG)

def get_redis_client() -> redis.Redis:
    """
    returns a redis clinet connected to the redis server
    """
    return redis.Redis(host="redis-db", port=6379, decode_responses=True)

rd = get_redis_client()
HGNC_URL = "https://storage.googleapis.com/public-download-files/hgnc/json/json/hgnc_complete_set.json"

@app.route('/data', methods=['POST'])
def pull_data():
    """
    pulls HGNC data from the external api and stores it in redis
    args: none
    returns: jsonify: json response descirbing the outcome of the function
    """
    logging.debug("Request to load HGNC data received.")
    response = requests.get(HGNC_URL)
    
    if response.status_code == 200:
        data = response.json()
        json_data = json.dumps(data)
        rd.set("hgnc_data", json_data)  # Store using a string key
        
        logging.info("HGNC data successfully fetched and stored in Redis.")
        return jsonify({"message": "HGNC data loaded successfully"}), 201
    else:
        logging.error(f"Failed to fetch HGNC data. Status code: {response.status_code}")
        return jsonify({"error": "Failed to fetch HGNC data"}), response.status_code


@app.route('/data', methods=['GET'])
def return_data():
    """
    Returns the cached HGNC data from Redis.

    Args: 
        None

    Returns:
        The cached data or an error message.
    """
    logging.debug("Request to retrieve HGNC data received.")
    cached_data = rd.get("hgnc_data")
    if cached_data:
        logging.info("Data retrieved from Redis cache.")
        return jsonify(json.loads(cached_data))
    return jsonify({"error": "No HGNC data available"}), 500


@app.route('/data', methods=['DELETE'])
def delete():
    """
    Deletes the cached HGNC data from Redis.

    Args: 
        None

    Returns:
        A message regarding the outcome of the function.
    """
    logging.debug("Request to delete HGNC data received.")
    deleted_data = rd.delete("hgnc_data")
    if deleted_data > 0:
        logging.info("HGNC data deleted from Redis cache.")
        return "", 204  # No Content (successful delete)
    logging.warning("No HGNC data found to delete.")
    return jsonify({"error": "No HGNC data found in Redis."}), 404  # Data not found



@app.route('/genes', methods=['GET'])
def get_all_genes():
    
    """
    retrieves all gene hgnc_id data from the cached redis data
    args: none
    returns: jsonify: A list of HGNC IDs or an error message.
    """
    """
    Assisted by chatgpt to ensure that the it is loaded directly as a json, and ensure structure to do analysis on
    """
    logging.debug("Request to list all HGNC IDs received.")
    try:
        if rd.exists("hgnc_data"):  # Check if the Redis key exists
            res = rd.get("hgnc_data")  # Get the data from Redis
            res_dict = json.loads(res)  # Directly load as JSON since it's a string
            
            
            if 'response' in res_dict and 'docs' in res_dict['response']:
                res_list = res_dict['response']['docs'] 
                
                
                id_list = [item['hgnc_id'] for item in res_list]
                logging.info(f"Retrieved {len(id_list)} gene IDs from Redis.")
                return jsonify(id_list) 
            else:
                logging.error("Missing expected 'response' or 'docs' in the data")
                return jsonify({"error": "Unexpected data format"}), 500
        else:
            return jsonify({"error": "No data found in Redis"}), 404
    except Exception as e:
        logging.error(f"Error in GET /genes: {e}")
        return jsonify({"error": "An error occurred while retrieving data"}), 500


@app.route('/genes/<hgnc_id>', methods=['GET'])
def gene_pull(hgnc_id: str):
    """
    retrieves a specific gene by its unique hgnc_id in redis
    
    args: hgnc_id (string) - the hgnc_id of the gene to retreive
    
    returns: jsonify - the hgnc statistiscs or an error message
    """
    logging.debug(f"Looking up gene with HGNC ID: {hgnc_id}")
    try:
        res = rd.get('hgnc_data')
        if res:
            res_dict = json.loads(res)  # Redis response is already a string, no need for decode
            logging.debug(f"Data from Redis: {res_dict}")

            # Loop through the list of gene data to find the gene with the matching hgnc_id
            for item in res_dict['response']['docs']:
                if item['hgnc_id'] == hgnc_id:
                    logging.info(f"Gene {hgnc_id} found and returned.")
                    return jsonify(item)
            logging.warning(f"Gene {hgnc_id} not found in dataset.")
            return jsonify({"error": f"Gene with HGNC ID {hgnc_id} not found"}), 404
        else:
            return jsonify({"error": "No data available"}), 404
    except Exception as e:
        logging.error(f"Error retrieving gene: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/jobs', methods=['POST'])
def create_job():
    """
    creates a new job with a unique identifier, in which the request must include 'start_date' and 'end_date' in the JSON body,
    both formatted as YYYY-MM-DD.

    The route will validate these inputs and, if valid, create a new job entry in Redis and queue it for processing.
    args: none
    
    returns: JSON respo0nse with job ID and submission status if successful, or an error message and failure code
    """
    logging.debug("Job creation request received.")
    try:
        data = request.get_json()

        # validation the date range data
        if not data or "start_date" not in data or "end_date" not in data:
            logging.warning("Job creation failed: missing dates.")
            return jsonify({
                "error": "You must provide both 'start_date' and 'end_date' in YYYY-MM-DD format."
            }), 400

        # generated using chatgpt to validate the time/date formatting
        try:
            datetime.strptime(data["start_date"], "%Y-%m-%d")
            datetime.strptime(data["end_date"], "%Y-%m-%d")
        except ValueError:
            logging.warning("Job creation failed: invalid date format.")
            return jsonify({
                "error": "Dates must be in YYYY-MM-DD format."
            }), 400

        
        job = add_job(data["start_date"], data["end_date"])
        logging.info(f"New job submitted: {job['id']} | Start: {data['start_date']} | End: {data['end_date']}")
        return jsonify({"job_id": job['id'], "status": "submitted"}), 201

    except Exception as e:
        logging.error(f"Exception in create_job: {str(e)}")
        return jsonify({"error": str(e)}), 500

        
@app.route('/jobs', methods=['GET'])
def list_jobs():
    """
    Lists all submitted job IDs sored in redis

    Returns:
        A Json response of job IDs stored in Redis. Or an error message if something goes wrong.
    """
    try:
        keys = jdb.keys('*')  # <- read from the actual jobs db
        job_ids = [k.decode() if isinstance(k, bytes) else k for k in keys]
        logging.info(f"Returning {len(job_ids)} jobs from Redis.")
        return jsonify({"jobs": job_ids}), 200
    except Exception as e:
        logging.error(f"Error listing jobs: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/jobs/<jobid>', methods=['GET'])
def get_job(jobid: str):
    """
    retrieves a specific job status by its unique jobid
    
    args: jobid (str) - the unique identifer of the job request
    
    returns: json response with job data and status, or an error message if not found.
    """
    logging.debug(f"Retrieving job status for {jobid}")
    try:
        job = get_job_by_id(jobid)
        if job:
            logging.info(f"Job {jobid} retrieved successfully.")
            return jsonify(job), 200
        else:
            logging.warning(f"Job {jobid} not found.")
            return jsonify({"error": "Job not found"}), 404
    except Exception as e:
        logging.error(f"Error retrieving job {jobid}: {e}")
        return jsonify({"error": "Internal server error"}), 500



from datetime import datetime

@app.route('/results/<jobid>', methods=['GET'])
def get_locus_types(jobid: str):
    """
    Given a job_id it outputs the count of specific locus_types for a given date approval range, conducted by a separate worker

    Args: jobid (str) - the unique identifer of the job request

    Returns: json flask response containing the result counter data, a status message if the job is not yet complete, or an error message.

    """
    logging.debug(f"Fetching analysis result for job {jobid}")
    try:
        result = results_db.get(jobid)
        if result:
            logging.info(f"Returning cached result for job {jobid}")
            return jsonify(json.loads(result)), 200

        job_data = r.hgetall(jobid)
        if not job_data:
            logging.warning(f"Job ID {jobid} not found.")
            return jsonify({"error": "Job ID not found"}), 404

        status = job_data.get("status", "unknown")
        if status != "complete":
            return jsonify({
                "message": f"Job {jobid} is not yet finished.",
                "status": status
            }), 202

        start_date_str = job_data.get("start_date")
        end_date_str = job_data.get("end_date")

        if not start_date_str or not end_date_str:
            logging.warning(f"Missing date fields for job {jobid}.")
            return jsonify({"error": "Job is missing start or end date"}), 400

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError:
            logging.warning(f"Invalid date format in job {jobid}.")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

        raw_data = json.loads(r.get("hgnc_data") or "{}")
        gene_list = raw_data.get("response", {}).get("docs", [])

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

        output = {
            "job_id": jobid,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "total_genes_counted": len(filtered_genes),
            "locus_type_counts": locus_counts
        }

        results_db.set(jobid, json.dumps(output))  # Cache in results DB
        logging.info(f"Generated result for job {jobid}: {len(filtered_genes)} genes counted.")
        return jsonify(output), 200

    except Exception as e:
        logging.error(f"Unexpected error in get_locus_types({jobid}): {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


if __name__ == "__main__":
    # main()
    app.run(debug=True, host='0.0.0.0')
