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

@app.route('/help', methods=['GET'])
def help():
    return jsonify({
        "routes": {
            "/plays/load": "POST - Load full play data into Redis and return it",
            "/data": "POST/GET/DELETE - Load, view, or delete basic play data subset",
            "/plays": "GET - Get all plays",
            "/plays/<play_id>": "GET - Get formation/playtype/description by ID",
            "/plays/rush": "GET - Get all rush plays",
            "/plays/pass": "GET - Get all pass plays",
            "/jobs": "POST - Submit a job for analysis",
            "/jobs": "GET - List all job IDs",
            "/jobs/<jobid>": "GET - Get job status",
            "/results/<jobid>": "GET - Return result of injury analysis",
            "/help": "GET - Describe all routes"
        }
    }), 200

@app.route('/data', methods = ['POST'])
def pull_data():
    """
    Loads NFL play-by-play data from a CSV file and stores it in Redis.
    Args: none
    Returns: jsonify: JSON response describing the outcome of the function.
    """
    logging.debug("Request to load NFL play-by-play data received.")

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
        df['Formation'] = df['Formation'].fillna('Unknown')  # Fill missing formation with 'Unknown'
        df['PlayType'] = df['PlayType'].fillna('Unknown')  # Fill missing play type with 'Unknown'
        df['Description'] = df['Description'].fillna('No description')  # Fill missing description
        df['RushDirection'] = df['RushDirection'].fillna('Unknown')  # Fill missing rush direction
        df['PassType'] = df['PassType'].fillna('Unknown')  # Fill missing pass type

        # Add play_id as a unique identifier for each row
        df['play_id'] = range(1, len(df) + 1)  # Sequential play_id (1-based index)

        # Convert the DataFrame to a list of dictionaries
        selected_columns = [
            'play_id', "GameId", "GameDate", "Quarter", "Minute", "Second", "OffenseTeam", "DefenseTeam",
            "Down", "ToGo", "YardLine", "SeriesFirstDown", "NextScore", "Description", "TeamWin",
            "SeasonYear", "Yards", "Formation", "PlayType", "IsRush", "IsPass", "IsIncomplete",
            "IsTouchdown", "PassType", "IsSack", "IsChallenge", "IsChallengeReversed", "Challenger",
            "IsMeasurement", "IsInterception", "IsFumble", "IsPenalty", "IsTwoPointConversion",
            "IsTwoPointConversionSuccessful", "RushDirection", "YardLineFixed", "YardLineDirection",
            "IsPenaltyAccepted", "PenaltyTeam", "IsNoPlay", "PenaltyType", "PenaltyYards"
        ]

        data = df[selected_columns].to_dict(orient='records')

        # Store the data in Redis
        rd.set("nfl_data", json.dumps(data))  # Store using a string key


        logging.info("NFL play-by-play data successfully fetched and stored in Redis.")
        return jsonify({"message": "NFL play-by-play data loaded successfully"}), 201

    except Exception as e:
        logging.error(f"Error loading data from CSV: {str(e)}")
        return jsonify({"error": f"Error loading data from CSV: {str(e)}"}), 500
        
@app.route('/data', methods = ['GET'])
def return_data():
    """
    Returns the cached NFL play-by-play data from Redis.
    Args: None
    Returns: The cached data or an error message.
    """
    logging.debug("Request to retrieve NFL play-by-play data received.")
    cached_data = rd.get("nfl_data")
    if cached_data:
        logging.info("Data retrieved from Redis cache.")
        return jsonify(json.loads(cached_data))
    return jsonify({"error": "No NFL play-by-play data available"}), 500


@app.route('/data', methods=['DELETE'])
def delete():
    """
    Deletes the cached NFL play-by-play data from Redis.
    Args: None
    Returns: A message regarding the outcome of the function.
    """
    logging.debug("Request to delete NFL play-by-play data received.")
    deleted_data = rd.delete("nfl_data")
    if deleted_data > 0:
        logging.info("NFL play-by-play data deleted from Redis cache.")
        return "", 204  # No Content (successful delete)
    logging.warning("No NFL play-by-play data found to delete.")
    return jsonify({"error": "No NFL play-by-play data found in Redis."}), 404  # Data not found


@app.route('/plays', methods=['GET'])
def load_plays():
    """
    Loads data from CSV into Redis and returns the data immediately.
    """
    try:
        # Load CSV data into a pandas DataFrame
        df = pd.read_csv(CSV_FILE_PATH)

        required_columns = ['Formation', 'PlayType', 'Description', 'RushDirection', 'PassType']
        if not all(col in df.columns for col in required_columns):
            return jsonify({"error": "CSV file is missing required columns"}), 400

        df['Formation'] = df['Formation'].fillna('Unknown')
        df['PlayType'] = df['PlayType'].fillna('Unknown')
        df['Description'] = df['Description'].fillna('No description')
        df['RushDirection'] = df['RushDirection'].fillna('Unknown')
        df['PassType'] = df['PassType'].fillna('Unknown')

        # ✅ Safely parse GameDate and convert to string
        df['GameDate'] = pd.to_datetime(df['GameDate'], errors='coerce')  # convert invalid dates to NaT
        df['GameDate'] = df['GameDate'].fillna(pd.Timestamp("1900-01-01"))  # fallback
        df['GameDate'] = df['GameDate'].dt.strftime('%Y-%m-%d')  # convert to string

        df['play_id'] = range(1, len(df) + 1)

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
        rd.set("nfl_data", json.dumps(data))

        return jsonify(data), 201

    except Exception as e:
        return jsonify({"error": f"Failed to load and return data: {str(e)}"}), 500



@app.route('/plays/<play_id>', methods=['GET'])
def get_play_structure(play_id):
    """
    Retrieves the formation, playtype, and description for a specific play_id.
    Also includes rush direction or pass type if applicable.
    """
    logging.debug(f"Request to retrieve play with play_id: {play_id}")
    cached_data = rd.get("nfl_data")
    
    if not cached_data:
        return jsonify({"error": "No play structure data available"}), 500

    data = json.loads(cached_data)

    for item in data:
        if str(item.get("play_id")) == str(play_id):
            play_info = {
                "play_id": item.get("play_id"),
                "formation": item.get("Formation"),
                "play_type": item.get("PlayType"),
                "description": item.get("Description")
            }

            if item.get("PlayType", "").lower() == "rush":
                play_info["rush_direction"] = item.get("RushDirection")
            elif item.get("PlayType", "").lower() == "pass":
                play_info["pass_type"] = item.get("PassType")

            return jsonify(play_info), 200

    return jsonify({"error": f"Play ID {play_id} not found"}), 404



@app.route('/plays/pass', methods=['GET'])
def pass_pull():
    """
    Retrieves a specific play structure based off the unique play_id.
    Args: play_id (str): The unique identifier of the play.
    Returns: A JSON response containing the play details or an error message if not found.
    """
    try:
        logging.debug(f"Request to retrieve play structure for passes received.")
        cached_data = rd.get("nfl_data")
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
        cached_data = rd.get("nfl_data")
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
    logging.debug("Job creation request received.")
    try:
        data = request.get_json()

        # Load and parse Redis data
        cached_data = json.loads(rd.get("nfl_data"))
        df = pd.DataFrame(cached_data)
        oldest_date = df['GameDate'].min()
        newest_date = df['GameDate'].max()

        if not data or "start_date" not in data or "end_date" not in data:
            logging.warning("Missing start_date or end_date — defaulting to full range.")
            data["start_date"] = oldest_date
            data["end_date"] = newest_date

        if "method" not in data:
            logging.warning("Job creation failed: missing method.")
            return jsonify({
                "error": "You must provide a method either plays/ or injuries/, refer to the documentation."
            }), 400

        try:
            datetime.strptime(data["start_date"], "%Y-%m-%d")
            datetime.strptime(data["end_date"], "%Y-%m-%d")

            if data["start_date"] < oldest_date or data["end_date"] > newest_date:
                raise ValueError("Dates are out of bounds.")
        except Exception as e:
            logging.warning(f"Invalid dates: {e}")
            return jsonify({
                "error": "Dates must be in YYYY-MM-DD format and within dataset range."
            }), 400

        if not (data["method"].startswith("plays/") or data["method"].startswith("injury/")):
            logging.warning("Job creation failed: invalid method.")
            return jsonify({
                "error": "Method must start with plays/ or injury/."
            }), 400

        job = add_job(data["start_date"], data["end_date"], data["method"])
        logging.info(f"New job submitted: {job['id']} | Method: {data['method']} | Start: {data['start_date']} | End: {data['end_date']}")
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


@app.route('/results/<jobid>', methods=['GET'])
def get_play_structure_types(jobid: str):
    """
    Given that the word "injured" appears in the description column, 
    calculate the injury rate for combinations of play formation and 
    rush direction (if rush) or pass type (if pass), within the job's 
    requested date range.

    Args: jobid (str): Unique job identifier

    Returns: JSON response with injury statistics, job status, or error message.
    """
    logging.debug(f"Fetching analysis result for job {jobid}")

    try:
        # ✅ Step 1: Check job metadata in DB 2
        job_data = jdb.hgetall(jobid)
        if not job_data:
            logging.warning(f"Job ID {jobid} not found.")
            return jsonify({"error": "Job ID not found"}), 404

        # ✅ Step 2: Ensure job is complete
        status = job_data.get("status", "unknown")
        if status != "complete":
            return jsonify({
                "message": f"Job {jobid} is not yet finished.",
                "status": status
            }), 202

        # ✅ Step 3: Try fetching cached result from DB 3
        result = results_db.get(jobid)
        if result:
            logging.info(f"Returning cached result for job {jobid}")
            return jsonify(json.loads(result)), 200

        logging.warning(f"Job {jobid} marked complete but no result found.")
        return jsonify({"error": "Job marked complete but result not cached."}), 500

    except Exception as e:
        logging.error(f"Unexpected error in get_play_structure_types({jobid}): {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
