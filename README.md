# Final Project: Injury analysis in NFL data

## This project contains:   

- A Python directory `src` containing three Python files:
  - `api.py`
  - `jobs.py`
  - `worker.py`
- A python directory 'test' containing three python test files:
  - `api.py`
  - `jobs.py`
  - `worker.py`
- The `Dockerfile` needed to build the image to run these containerized programs
- A `docker-compose.yml` file to automate the deployment of the Flask app, Redis database, and background worker together
- A `requirements.txt` that lists non-standard Python libraries that must be installed for this project
- A `data` directory filled with a `.gitcanary` datafile as a placeholder for data of future projects
- A `diagram.png` diagram that illustrate the more interesting or important parts of the web app


### One objective of this assignment is to use the ```src``` python directory to run the ensuing functions:    
#### From `api.py`:   

```def get_redis_client()```, ```def helf()```, ```def pull_data()```, ```def get_data()```, ```def delete()```, ```def load_plays(hgnc_id: str)```, ```def get_play_structure()```, ```def pass_pull()```, ```def rush_pull()```, ```def create_job()```, ```def list_jobs()```, ```def get_job(jid)```, ```def get_injury_summary()```

#### From `jobs.py`:

```def _generate_jid()```, ```def _instantiate_job(jid, status, start, end)```, ```def _save_job(jid, job_dict)```, ```def _queue_job(jid)```, ```def add_job(start, end, status="submitted")```, ```def get_job_by_id(jid)```, and ```def update_job_status(jid, status)```   

#### From `worker.py`:

```def run_worker_job_logic(job_id: str) -> None:``` & ```def do_work(job_id)```



### This can be checked using the ```test``` python directory to run the ensuing functions:   
#### From `test_api.py`:   

```def test_pull_data():```, ```def test_return_data():```, ```def test_get_genes():```, ```def test_specific_gene_format():```, ```def test_create_job():```, ```def test_job_status():```, and ```def test_get_job_result():```   

#### From `test_jobs.py`:   

```def test_generate_jid_format():```, ```def test_instantiate_job_structure():```, ```def test_save_and_get_job():```, ```def test_update_job_status():```, ```def test_add_job():```, and ```def teardown_module(module):```   

#### From `test_worker.py`:   

```def setup_mock_data(job_id: str, start: str, end: str):``` & ```def test_do_work_creates_results_and_updates_status():```




#### Purpose:
These functions are used to build our redis database, and then store, return, and later delete the data. We are to then run flask API routes to extract various data analysis regarding the gene data to provide the user with data for their given hgnc_id request. Finally, the api.py scripts are meant to call upon the jobs.py and worker.py scripts to create an asynchronous job queue system, where user submitted jobs are queued and then processed in the background by a worker, while being tracked through Redis.

We chose to pursue a fully functional web app that supported an admin function as well. Therefore, there are routes that will run in real time, such as posting the data or deleting the data, in case an admin wants to change data. Then there are user friendly routes such as creating a job. These are run in succession asyncronously and do not have the same capacity as the admin functions.

Additionally, the goal is for the functions: ```def pull_data()```, ```def return_data()```, ```def delete()```, ```def get_all_genes()```, ```def get_play_structure(play_id: str)```, ```def create_job()```, ```def list_jobs()```, and ```def get_job(jobid: str)```; to set up Flask API routes with ```@app.route``` to then call upon these functions using curl commands to run from the server for various endpoints, with the data in the redis database.  

Finally we run integration and unit tests using the `test` directory to ensure the smooth programming processes are working as expected.
## Routes
The following route endpoints correlate to the following functions:   

```@app.route('/data', methods = ['POST'])``` is used to run ```def pull_data()```

```@app.route('/data', methods = ['GET'])``` is used to run ```def return_data()```

```@app.route('data', methods=['DELETE'])``` is used to run ```def delete()```

```@app.route('/plays', methods=['GET'])``` is used to run  ```def load_plays()```   

```@app.route('/plays/<play_id>', methods=['GET'])``` is used to run ```def get_play_structure(hgnc_id: str)```   

```@app.route('/jobs', methods=['POST'])``` is used to run ```def create_job()```   

```@app.route('/jobs', methods=['GET'])``` is used to run ```def list_jobs()```  

```@app.route('/jobs/<jobid>', methods=['GET'])``` is used to run ```def get_job(jobid: str)```   

```@app.route('/results/<jobid>', methods=['GET'])``` is used to run ```def get_injury_summary(jobid: str)```   


#### Additionally, in Worker.py

The HotQueue ```@q.worker``` decorator is used to watch the queue and pull off Job IDs as they arrive.


## Data
The data this projet is analyzing is the complete list of every named gene. The Human Genome Organization has named every genome with a meaningul name. This dataset includes all of those genomes, where they are, what ehy're named, symbols for the genome,  various Id's and other descriptors. Used here in json format this data is comprehensive and crucial to understanding human genomes. Having this public database is crucial, and these functions help users to identify and search for individual genomes.

The data can be accessed through the HGNC archive at the following link:(https://nflsavant.com/about.php) [https://nflsavant.com/about.php]

Where the user can then download the specific csv data from that website. 

It is then built into the redis database using the ```def pull_data()``` and ```def get_redis_client()``` functions with the downloaded URL being set above:    
```rd = get_redis_client()   

rd = get_redis_client()   
<!-- HGNC_URL = "https://storage.googleapis.com/public-download-files/hgnc/json/json/hgnc_complete_set.json" -->
```   
 
The ```pull_data()``` function fetches the dataset and stores it in Redis for access by all route

## Deploying the App from docker Compose
A container for this code can be made with the following docker commands using the file and contents of: ```docker-compose.yml```   

within your desired directory run: ```docker-compose up --build``` This will start the container, as well as build it in the default driver.   

you can check that both your flask API and redis database are setup using ```docker ps``` to output what is currently running   

To run pytest please executing the following:   

`docker exec -it flask-app env PYTHONPATH=/code pytest /code/test`


### Running as a Flask App:
The line ```app = Flask(__name__)``` allows the file to turn into a Flask API server. From there the user should open a second terminal window and naviaget back to the same folder that holds these python scripts and where the generated flask api server is currently running. Then, the user can run the following structure to call upon the routes that were written in the api.py file in the localhost and default port = 5000: ```curl -X GET "http://127.0.0.1:5000/data"``` where, ```127.0.0.1:5000``` is generated from the ```* running on ...``` line in the terminal window in which the Flask API is running.  

/data can be replaced with any of the endpoints given below depending on the desired function   

## Running with Kubernetes containers locally
To run this project with Kubernetes containers locally, you should first cd into the kubernetes directory and then run ```kubectl apply -f test/```, and then run a port forward command like: ```kubectl port-forward service/flask-service 8080:80```. Then to test whether the app is running, try ```curl -X GET "http://127.0.0.1:8080/help"```. Keep in mind that for the next set of commands, you need to replace the port ```5000``` with the port ```8000```.

### Parameters

To make these queue based job requests it is important that the user provides specific parameters to conduct analysis over datapoints to their specifications. For this dataset, that means giving a range of gene approval dates that the user would like to analyze the results of.

For example, if a user wants to request every gene that was approved between February 2nd 2010 and March 20th 2015

they would use:   

```curl -X POST "http://127.0.0.1:5000/jobs" \ -H "Content-Type: application/json" \ -d '{"start_date": "2010-02-10", "end_date": "2015-03-20"}'```   

and it would request the gene data that was approved between those two dates.

Furthermore, for the given curl command:   

```curl -X GET http://127.0.0.1:5000/results/82916f6d-af42-42b5-bd18-07fc762fc48f```   

the user will take the previous filter job command by start and end dates in which the gene data was approved, and conduct a counting analysis to determine the number of locus_types present within that timeframe.

### Output  
The output will be the analysis of the downloaded data from our functions.   

This will include:   
- ```curl -X POST "http://127.0.0.1:5000/plays"``` - this will output a message on if the data was stored correctly   

    example code output:  
    ```json
    {
      "message": "NFL data loaded successfully"
    }
    ```

- ```curl -X GET "http://127.0.0.1:5000/plays"``` - this will ouput the entire dataset that has been stored in redis

    example code snippet output:  
    ```json
     {
    "Challenger": NaN,
    "DefenseTeam": "CLE",
    "Description": "TIMEOUT #1 BY PIT AT 03:03.",
    "Down": 0,
    "Formation": "UNDER CENTER",
    "GameDate": "2024-12-08",
    "GameId": 2024120804,
    "IsChallenge": 0,
    "IsChallengeReversed": 0,
    "IsFumble": 0,
    "IsIncomplete": 0,
    "IsInterception": 0,
    "IsMeasurement": 0,
    "IsNoPlay": 0,
    "IsPass": 0,
    "IsPenalty": 0,
    "IsPenaltyAccepted": 0,
    "IsRush": 0,
    "IsSack": 0,
    "IsTouchdown": 0,
    "IsTwoPointConversion": 0,
    "IsTwoPointConversionSuccessful": 0,
    "Minute": 3,
    "NextScore": 0,
    "OffenseTeam": "PIT",
    "PassType": "Unknown",
    "PenaltyTeam": NaN,
    "PenaltyType": NaN,
    "PenaltyYards": 0,
    "PlayType": "TIMEOUT",
    "Quarter": 4,
    "RushDirection": "Unknown",
    "SeasonYear": 2024,
    "Second": 3,
    "SeriesFirstDown": 1,
    "TeamWin": 0,
    "ToGo": 0,
    "YardLine": 0,
    "YardLineDirection": "OPP",
    "YardLineFixed": 100,
    "Yards": 0,
    "play_id": 2258
  },
    ```

- ```curl -X DELETE "http://127.0.0.1:5000/plays"``` - this will output nothing, and the redis database will be cleared of data by deleting it   

- ```curl -X GET "http://127.0.0.1:5000/plays/2257"``` - this will output description and other information regarding a specific HGNC gene id

    example code snippet output:  
    ```json
  {
    "Challenger": NaN,
    "DefenseTeam": "ARI",
    "Description": "(5:46) (SHOTGUN) 26-Z.CHARBONNET UP THE MIDDLE TO ARI 49 FOR NO GAIN (43-J.LUKETA).",
    "Down": 1,
    "Formation": "SHOTGUN",
    "GameDate": "2024-12-08",
    "GameId": 2024120807,
    "IsChallenge": 0,
    "IsChallengeReversed": 0,
    "IsFumble": 0,
    "IsIncomplete": 0,
    "IsInterception": 0,
    "IsMeasurement": 0,
    "IsNoPlay": 0,
    "IsPass": 0,
    "IsPenalty": 0,
    "IsPenaltyAccepted": 0,
    "IsRush": 1,
    "IsSack": 0,
    "IsTouchdown": 0,
    "IsTwoPointConversion": 0,
    "IsTwoPointConversionSuccessful": 0,
    "Minute": 5,
    "NextScore": 0,
    "OffenseTeam": "SEA",
    "PassType": "Unknown",
    "PenaltyTeam": NaN,
    "PenaltyType": NaN,
    "PenaltyYards": 0,
    "PlayType": "RUSH",
    "Quarter": 4,
    "RushDirection": "CENTER",
    "SeasonYear": 2024,
    "Second": 46,
    "SeriesFirstDown": 0,
    "TeamWin": 0,
    "ToGo": 10,
    "YardLine": 49,
    "YardLineDirection": "OPP",
    "YardLineFixed": 51,
    "Yards": 0,
    "play_id": 2257
  }
    ```

- ```curl -X POST http://127.0.0.1:5000/jobs \ -H "Content-Type: application/json" \ -d '{"start_date": "2024-06-23", "end_date": "2025-01-10"}'``` - this will create a job submission request within a given list of specified gene approval dates that the user submits. It is important to know that if the jobs request is missing data, it will default to the range of the dataset. Therefore the following is a correct request as well ```curl -X POST http://127.0.0.1:5000/jobs \ -H "Content-Type: application/json" \ -d '{}'``` - This functionality is different than what we have studies in class, but it makes sense for a dataset like this.
 
    example code output:  
    ```json
    {
      "job_id": "82916f6d-af42-42b5-bd18-07fc762fc48f",
      "status": "submitted"
    }
    ```

- ```curl -X GET "http://127.0.0.1:5000/jobs"``` - this will return all the current jobs that have been submitted by the user.
  
    example code output:  
    ```json
    {
      "jobs": [
        "fbe83c5c-bad4-4b85-a9c3-d1a8bb45e83b"
      ]
    }
    ```

- ```curl -X GET http://127.0.0.1:5000/jobs/82916f6d-af42-42b5-bd18-07fc762fc48f``` - this will return the status of a specified job request as: submitted, in progress, or complete   

    example code output:  
    ```json
    {
      "end": "2025-01-20",
      "id": "82916f6d-af42-42b5-bd18-07fc762fc48f",
      "start": "2024-06-10",
      "status": "complete"
    }
    ```
- ```curl -X GET http://127.0.0.1:5000/results/82916f6d-af42-42b5-bd18-07fc762fc48f``` - this will return the status of a specified job request as: submitted, in progress, or complete   

    example code output:  
    ```json
    {
  "end_date": "2020-01-01",
  "job_id": "7da3281d-d106-43e2-a54f-de68ebe3b5a7",
  "injury_combo_counts": {
      ["Some injury data"]
  },
  "start_date": "2005-01-01",
  "total_genes_counted": 23271
  }
    ```


## Diagram

A diagram of the process described is seen here:

![Diagram of App Architecture](diagram.png)

## Chatgpt acknowledgment
Chatgpt was used to assist in this code primarily in the genes script to ensure consistency in data structure;   

it was also used to help build the ```test_worker.py``` functions to ensure that the error test cases were being read properly


it was also used to help throughout with some of the redis formatting (indentations and structure);   

finally it was used to build part of the ```def create_job():``` function to help build and verify the date times for the range indicator. This was done because I wanted to ensure the correct format.




