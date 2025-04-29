# test/test_worker.py

import pytest
import json
from datetime import datetime
from worker import do_work, rd, jdb, results_db, update_job_status
from jobs import _instantiate_job, _save_job

def setup_mock_data(job_id: str, start: str, end: str):
    job = _instantiate_job(job_id, "submitted", start, end)
    _save_job(job_id, job)

    mock_gene_data = {
        "response": {
            "docs": [
                {
                    "hgnc_id": "HGNC:1",
                    "date_approved_reserved": "2012-01-01",
                    "locus_type": "protein-coding gene"
                },
                {
                    "hgnc_id": "HGNC:2",
                    "date_approved_reserved": "2013-06-15",
                    "locus_type": "pseudogene"
                },
                {
                    "hgnc_id": "HGNC:3",
                    "date_approved_reserved": "2009-01-01",
                    "locus_type": "ncRNA"
                }
            ]
        }
    }
    rd.set("hgnc_data", json.dumps(mock_gene_data))


"""
assisted using chatgpt because the pytest wasnt working
"""
@pytest.mark.integration
def test_do_work_creates_results_and_updates_status():
    job_id = "test-job-123"
    setup_mock_data(job_id, "2010-01-01", "2014-01-01")
    
    from worker import run_worker_job_logic

    run_worker_job_logic(job_id)


    result = results_db.get(job_id)
    assert result is not None
    result_dict = json.loads(result)
    assert result_dict["job_id"] == job_id
    assert result_dict["total_genes_counted"] == 2
    assert "protein-coding gene" in result_dict["locus_type_counts"]
    assert "pseudogene" in result_dict["locus_type_counts"]
    assert "ncRNA" not in result_dict["locus_type_counts"]  # Out of date range

    job = json.loads(jdb.get(job_id))
    assert job["status"] == "complete"
