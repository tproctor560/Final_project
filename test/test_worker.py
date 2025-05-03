import pytest
import json
from datetime import datetime
from worker import run_worker_job_logic, rd, jdb, results_db
from jobs import _instantiate_job, _save_job

def setup_mock_nfl_data(job_id: str, start: str, end: str):
    # Setup job in Redis
    job = _instantiate_job(job_id, "submitted", start, end)
    _save_job(job_id, job)

    # Sample mock NFL data covering various formations/play types and some with "injured"
    mock_nfl_data = [
        {
            "GameDate": "01/01/12",
            "Description": "Player was injured on the play.",
            "Formation": "Shotgun",
            "PlayType": "RUSH",
            "RushDirection": "CENTER"
        },
        {
            "GameDate": "01/01/12",
            "Description": "Standard pass.",
            "Formation": "I-Form",
            "PlayType": "PASS",
            "PassType": "SHORT RIGHT"
        },
        {
            "GameDate": "01/01/12",
            "Description": "Another rush, no injury.",
            "Formation": "Shotgun",
            "PlayType": "RUSH",
            "RushDirection": "CENTER"
        },
        {
            "GameDate": "01/01/15",  # outside date range
            "Description": "Injured player.",
            "Formation": "Singleback",
            "PlayType": "PASS",
            "PassType": "DEEP LEFT"
        }
    ]
    rd.set("nfl_data", json.dumps(mock_nfl_data))

@pytest.mark.integration
def test_run_worker_job_logic_creates_results_and_updates_status():
    job_id = "test-nfl-job-001"
    setup_mock_nfl_data(job_id, "2010-01-01", "2014-01-01")

    run_worker_job_logic(job_id)

    # Validate job status
    job = json.loads(jdb.get(job_id))
    assert job["status"] == "complete"

    # Validate result exists
    result = results_db.get(job_id)
    assert result is not None

    result_dict = json.loads(result)
    assert result_dict["job_id"] == job_id
    assert result_dict["start_date"] == "2010-01-01"
    assert result_dict["end_date"] == "2014-01-01"
    
    # Confirm injury_combo_counts structure
    counts = result_dict["injury_combo_counts"]
    assert isinstance(counts, dict)

    # Check one of the known keys
    key = "Formation: Shotgun; PlayType: RUSH; Direction: LEFT"
    assert key in counts
    assert counts[key]["total_plays"] == 2
    assert counts[key]["injury_plays"] == 1
    assert counts[key]["injury_percentage"] == 50.0
