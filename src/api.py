import requests
import uuid
import math
import logging
import pandas as pd
from flask import Flask, request, jsonify
import json
import os
import redis
from datetime import datetime
from jobs import add_job, get_job_by_id, jdb, results_db

app = Flask(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level))

def get_redis_client() -> redis.Redis:
    """
    Returns a redis client connected to the redis server.
    """
    return redis.Redis(host="redis-db", port=6379, decode_responses=True)

rd = get_redis_client()
CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'pbp-2024.csv')

@app.route("/data", methods=["POST"])
def load_data():
    """
    Loads NFL play-by-play data from a CSV file into Redis.
    """
    logging.debug("Loading NFL play-by-play data received.")
    try:
        # Load CSV data into a pandas DataFrame
        df = pd.read_csv(CSV_FILE_PATH)

        # Print out the column names for debugging purposes
        logging.debug(f"CSV Columns: {df.columns.tolist()}")

        # Check if the CSV contains the necessary columns
        required_columns = ['Formation', 'PlayType', 'Description', 'RushDirection', 'PassType']
        if not all(col in df.columns for col in required_columns):
            logging.error(f"CSV file is missing required columns. Found columns: {df.columns}")
            return jsonify({"error": "CSV file is missing required columns"}), 400

        # Handle missing or invalid data
        df['Formation'] = df['Formation'].fillna('Unknown')
        df['PlayType'] = df['PlayType'].fillna('Unknown')
        df['Description'] = df['Description'].fillna('No description')
        df['RushDirection'] = df['RushDirection'].fillna('Unknown')
        df['PassType'] = df['PassType'].fillna('Unknown')
        if not pd.api.types.is_datetime64_any_dtype(df['GameDate']):
            df['GameDate'] = pd.to_datetime(df['GameDate'])

        # Add unique play_id
        df['play_id'] = range(1, len(df) + 1)

        # Select relevant columns
        selected_columns = [
            'play_id', 'GameId', 'GameDate', 'Quarter', 'Minute', 'Second', 'OffenseTeam', 'DefenseTeam',
            'Down', 'ToGo', 'YardLine', 'SeriesFirstDown', 'NextScore', 'Description', 'TeamWin',
            'SeasonYear', 'Yards', 'Formation', 'PlayType', 'IsRush', 'IsPass', 'IsIncomplete',
            'IsTouchdown', 'PassType', 'IsSack', 'IsChallenge', 'IsChallengeReversed', 'Challenger',
            'IsMeasurement', 'IsInterception', 'IsFumble', 'IsPenalty', 'IsTwoPointConversion',
            'IsTwoPointConversionSuccessful', 'RushDirection', 'YardLineFixed', 'YardLineDirection',
            'IsPenaltyAccepted', 'PenaltyTeam', 'IsNoPlay', 'PenaltyType', 'PenaltyYards'
        ]

        data = df[selected_columns].to_dict(orient='records')

        # Store data in Redis
        rd.set("hgnc_data", json.dumps(data))

        logging.info("NFL play-by-play data successfully loaded into Redis.")
        return jsonify({"message": "NFL play-by-play data loaded successfully"}), 201

    except Exception as e:
        logging.error(f"Error loading data from CSV: {str(e)}")
        return jsonify({"error": f"Error loading data from CSV: {str(e)}"}), 500

@app.route("/plays", methods=["GET"])
def get_plays():
    """
    Returns all NFL play-by-play data from Redis.
    """
    try:
        data = rd.get("hgnc_data")
        if not data:
            return jsonify({"error": "No data found in Redis. Please load data first."}), 404

        plays = json.loads(data)
        return jsonify(plays), 200

    except Exception as e:
        logging.error(f"Error retrieving plays from Redis: {str(e)}")
        return jsonify({"error": "Failed to fetch plays"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)



@app.route('/plays/play_structure', methods=['GET' 'DELETE'])
def get_all_genes():
    """
    Retrieves the formation, playtype, and description for each play, 
    additionally it returns the rush direction if the playtype=rush 
    and the pass type if the playtype=pass.
    Returns: A JSON response with all play data.
    """
    logging.debug("Request to retrieve all play structure data received.")
    cached_data = rd.get("hgnc_data")
    if cached_data:
        logging.info("Data retrieved from Redis cache.")
        data = json.loads(cached_data)
        play_data = []

        for item in data:
            play_info = {
                "play_id": item.get("play_id"),  # Use the play_id from the data
                "formation": item.get("Formation"),
                "play_type": item.get("PlayType"),
                "description": item.get("Description"),
            }

            if item.get("play_type") == "rush":
                play_info["rush_direction"] = item.get("RushDirection")
            elif item.get("play_type") == "pass":
                play_info["pass_type"] = item.get("PassType")

            play_data.append(play_info)

        return jsonify(play_data), 200

    return jsonify({"error": "No play structure data available"}), 500


@app.route('/plays/<play_id>', methods=['GET'])
def gene_pull(play_id: str):
    """
    Retrieves a specific play structure based off the unique play_id.
    Args: play_id (str): The unique identifier of the play.
    Returns: A JSON response containing the play details or an error message if not found.
    """
    logging.debug(f"Request to retrieve play structure for play_id: {play_id} received.")
    cached_data = rd.get("hgnc_data")
    if cached_data:
        logging.info("Data retrieved from Redis cache.")
        data = json.loads(cached_data)
        
        # Search for the play with the provided play_id
        for item in data:
            if item.get("play_id") == play_id:
                return jsonify(item), 200

    return jsonify({"error": f"Play with id {play_id} not found"}), 404


@app.route('/plays/pass', methods=['GET'])
def pass_pull():
    """
    Retrieves a specific play structure based off the unique play_id.
    Args: play_id (str): The unique identifier of the play.
    Returns: A JSON response containing the play details or an error message if not found.
    """
    try:
        logging.debug(f"Request to retrieve play structure for passes received.")
        cached_data = rd.get("hgnc_data")
        if cached_data:
            logging.info("Data retrieved from Redis cache.")
            data = json.loads(cached_data)
            
            pass_list = []
            
            # Search for the play with the provided play_id
            for item in data:
                if item.get("PlayType") == "PASS":
                    pass_list.append(item)
            return jsonify(pass_list), 200
    except Exception as e:
        return jsonify({"error": f"Error: {e}"}), 404
    

@app.route('/plays/rush', methods=['GET'])
def rush_pull():
    """
    Retrieves a specific play structure based off the unique play_id.
    Args: play_id (str): The unique identifier of the play.
    Returns: A JSON response containing the play details or an error message if not found.
    """
    try:
        logging.debug(f"Request to retrieve play structure for rush received.")
        cached_data = rd.get("hgnc_data")
        if cached_data:
            logging.info("Data retrieved from Redis cache.")
            data = json.loads(cached_data)
            
            rush_list = []
            
            # Search for the play with the provided play_id
            for item in data:
                if item.get("PlayType") == "RUSH":
                    rush_list.append(item)
            return jsonify(rush_list), 200
    except Exception as e:
        return jsonify({"error": f"Error: {e}"}), 404


@app.route('/jobs', methods=['POST'])
def create_job():
    """
    Creates a new job with a unique identifier, where the request must include 'start_date' 
    and 'end_date' in the JSON body, both formatted as YYYY-MM-DD.
    
    The route will validate these inputs and, if valid, create a new job entry in Redis 
    and queue it for processing.
    
    Args: none
    Returns: JSON response with job ID and submission status if successful, 
            or an error message and failure code.
    """
    logging.debug("Job creation request received.")
    try:
        data = request.get_json()
        cached_data = rd.get("hgnc_data")
        oldest_date = cached_data['GameDate'].min()  
        newest_date = cached_data['GameDate'].max()

        # Validate the date range data
        if not data or "start_date" not in data or "end_date" not in data:
            logging.warning("Job creation failed: missing dates.")
            logging.info("Setting the dates to the maximum and minimum of this dataset")
            data["start_date"] = oldest_date
            data["end_date"] = newest_date
            
        if "method" not in data:
            logging.warning("Job creation failed: missing method")
            return jsonify({
                "error": "You must provide a method either plays/, or injuries/, refer to the documentation for more help"
            }), 400

        # Validate the date format
        try:
            datetime.strptime(data["start_date"], "%Y-%m-%d")
            datetime.strptime(data["end_date"], "%Y-%m-%d")
            
            if data["start_date"] < oldest_date or data["end_date"] > newest_date:
                logging.warning("Dates are not in the bounds")
                raise Exception("Dates are not in the bounds")
        except Exception as e:
            logging.warning(f"Job creation failed: invalid date format. Check {e}")
            return jsonify({
                "error": "Dates must be in YYYY-MM-DD format. and must be in the bounds"
            }), 400
            
        if not data["method"].startswith("plays/") or not data["method"].startswith("injury/"):
            logging.warning("Job creation failed: invalid method.")
            return jsonify({
                "error": "Methods should start with plays/ or injury/, and then specific play ids after"
            }), 400
        
        job = add_job(data["start_date"], data["end_date"], data["method"])
        logging.info(f"New job submitted: {job['id']} | Method: {data["method"]} | Start: {data['start_date']} | End: {data['end_date']}")
        return jsonify({"job_id": job['id'], "status": job["status"]}), 201

    except Exception as e:
        logging.error(f"Exception in create_job: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/jobs', methods=['GET'])
def list_jobs():
    """
    Lists all submitted job IDs stored in Redis.
    
    Returns: A JSON response of job IDs stored in Redis, or an error message if something goes wrong.
    """
    try:
        keys = jdb.keys('*')  # <- read from the actual jobs DB
        job_ids = [k.decode() if isinstance(k, bytes) else k for k in keys]
        logging.info(f"Returning {len(job_ids)} jobs from Redis.")
        return jsonify({"jobs": job_ids}), 200
    except Exception as e:
        logging.error(f"Error listing jobs: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/jobs/<jobid>', methods=['GET'])
def get_job(jobid: str):
    """
    Retrieves a specific job status by its unique jobid.
    
    Args: jobid (str) - The unique identifier of the job request.
    
    Returns: JSON response with job data and status, or an error message if not found.
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


#TODO: Replace this
@app.route('/results/<jobid>', methods=['GET'])
def get_locus_types(jobid: str):
    """
    Given that the word "injured" appears in the description column, 
    calculate the injury rate for specific combinations of play formation 
    and rush direction, and output this as a percentage of the total number 
    of plays for that combo.
    
    Args: jobid (str): The unique identifier of the job request.
    
    Returns: JSON Flask response containing the result counter data, 
            a status message if the job is not yet complete, or an error message.
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

        raw_data = json.loads(r.get("hgnc_data") or "{}")
        nfl_data = raw_data.get("response", {}).get("docs", [])

        # Filter plays that contain the word "injured" in the description
        filtered_plays = [play for play in nfl_data if "injured" in play.get("description", "").lower()]
        
        # Count occurrences by formation type and rush direction
        injury_counts = {}
        for play in filtered_plays:
            formation = play.get("formation", "Unknown")
            rush_direction = play.get("rush_direction", "Unknown")
            combo = f"{formation} - {rush_direction}"
            
            injury_counts[combo] = injury_counts.get(combo, 0) + 1
        
        # Calculate the injury rate percentage for each combo
        total_combos = len(filtered_plays)
        injury_percentage = {
            combo: (count / total_combos) * 100 if total_combos else 0
            for combo, count in injury_counts.items()
        }

        output = {
            "job_id": jobid,
            "injury_percentage": injury_percentage,
            "total_plays_counted": total_combos
        }

        results_db.set(jobid, json.dumps(output))  # Cache in results DB
        logging.info(f"Generated result for job {jobid}: {total_combos} injuries counted.")
        return jsonify(output), 200

    except Exception as e:
        logging.error(f"Unexpected error in get_locus_types({jobid}): {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
    load_data_from_csv()
