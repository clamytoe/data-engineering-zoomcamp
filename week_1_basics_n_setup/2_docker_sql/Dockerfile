FROM python:3.10.9

RUN apt-get install wget
RUN pip install pandas psycopg2 pyarrow sqlalchemy tqdm

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]