import requests
import time

BASE = "http://127.0.0.1:5000"

def test_help():
    res = requests.get(f"{BASE}/help")
    assert res.status_code == 200
    assert "/data" in res.json()["routes"]

def test_pull_data():
    res = requests.post(f"{BASE}/data")
    assert res.status_code in [200, 201]

def test_return_data():
    res = requests.get(f"{BASE}/data")
    assert res.status_code == 200
    assert isinstance(res.json(), list)
    assert len(res.json()) > 0

def test_get_play_by_id():
    data = requests.get(f"{BASE}/data").json()
    sample_id = data[0]["play_id"]
    res = requests.get(f"{BASE}/plays/{sample_id}")
    assert res.status_code == 200
    assert "formation" in res.json()

def test_pass_plays():
    res = requests.get(f"{BASE}/plays/pass")
    assert res.status_code == 200
    assert isinstance(res.json(), list)

def test_rush_plays():
    res = requests.get(f"{BASE}/plays/rush")
    assert res.status_code == 200
    assert isinstance(res.json(), list)

def test_create_job():
    res = requests.post(f"{BASE}/jobs", json={
        "start_date": "2010-01-01",
        "end_date": "2024-01-01",
        "method": "injury/count"
    })
    assert res.status_code in [200, 201]
    global job_id
    job_id = res.json()["job_id"]

def test_job_status():
    res = requests.get(f"{BASE}/jobs/{job_id}")
    assert res.status_code in [200, 202]
    assert "status" in res.json()

def test_get_job_result():
    time.sleep(3)
    res = requests.get(f"{BASE}/results/{job_id}")
    assert res.status_code in [200, 202]
