import requests

BASE = "http://127.0.0.1:5000"

def test_pull_data():
    res = requests.post(f"{BASE}/data")
    assert res.status_code in [200, 201]

def test_return_data():
    res = requests.get(f"{BASE}/data")
    assert res.status_code == 200
    assert isinstance(res.json(), dict)

def test_get_genes():
    res = requests.get(f"{BASE}/genes")
    assert res.status_code == 200
    assert isinstance(res.json(), list)

def test_specific_gene_format():
    gene_list = requests.get(f"{BASE}/genes").json()
    if gene_list:
        sample_id = gene_list[0]
        res = requests.get(f"{BASE}/genes/{sample_id}")
        assert res.status_code == 200
        assert isinstance(res.json(), dict)

def test_create_job():
    res = requests.post(f"{BASE}/jobs", json={
        "start_date": "2010-01-01",
        "end_date": "2015-01-01"
    })
    assert res.status_code in [200, 201]
    assert "job_id" in res.json()
    global job_id  # store it for the next test
    job_id = res.json()["job_id"]

def test_job_status():
    res = requests.get(f"{BASE}/jobs")
    assert res.status_code == 200
    assert isinstance(res.json()["jobs"], list)

def test_get_job_result():
    import time
    time.sleep(2)
    res = requests.get(f"{BASE}/jobs")
    jid = res.json()["jobs"][0]
    result = requests.get(f"{BASE}/results/{jid}")
    assert result.status_code in [200, 202]  # 202 if not yet complete
