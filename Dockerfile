
FROM python:3.12

WORKDIR /code

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/api.py /code/api.py
COPY src/jobs.py /code/jobs.py
COPY src/worker.py /code/worker.py
COPY data/pbp-2024.csv /code/data/pbp-2024.csv

RUN chmod +x /code/api.py

EXPOSE 5000

CMD ["python", "api.py"]

