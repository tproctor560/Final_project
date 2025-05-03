import pytest
import uuid
import json
from jobs import (
    _generate_jid,
    _instantiate_job,
    _save_job,
    get_job_by_id,
    update_job_status,
    add_job,
    jdb,
    q
)

def test_generate_jid_format():
    jid = _generate_jid()
    assert isinstance(jid, str)
    assert len(jid) == 36  # UUID format

def test_instantiate_job_structure():
    jid = "test-jid"
    job = _instantiate_job(jid, "submitted", "2020-01-01", "2021-01-01")
    assert isinstance(job, dict)
    assert job['id'] == jid
    assert job['status'] == "submitted"
    assert job['start'] == "2020-01-01"
    assert job['end'] == "2021-01-01"

def test_save_and_get_job():
    jid = "test-save-job"
    job_data = {
        "id": jid,
        "status": "submitted",
        "start": "2015-01-01",
        "end": "2016-01-01"
    }
    _save_job(jid, job_data)
    stored = get_job_by_id(jid)
    assert stored == job_data

def test_update_job_status():
    jid = "test-status-job"
    job_data = {
        "id": jid,
        "status": "submitted",
        "start": "2010-01-01",
        "end": "2012-01-01"
    }
    _save_job(jid, job_data)
    update_job_status(jid, "running")
    updated = get_job_by_id(jid)
    assert updated["status"] == "running"

def test_add_job_creates_valid_job():
    job = add_job("2013-01-01", "2014-01-01")
    assert "id" in job
    assert job["status"] == "submitted"
    assert job["start"] == "2013-01-01"
    assert job["end"] == "2014-01-01"
    # Confirm job was saved to DB
    assert isinstance(get_job_by_id(job["id"]), dict)
    # Confirm job was added to queue
    assert job["id"] in q.queue

def teardown_module(module):
    """Cleanup test jobs from Redis DBs and queue"""
    keys = jdb.keys("test-*")
    for k in keys:
        jdb.delete(k)

    # Remove any test job IDs from the queue
    test_ids = [b.decode() if isinstance(b, bytes) else b for b in q.queue]
    for jid in test_ids:
        if jid.startswith("test-"):
            q.queue.remove(jid)
