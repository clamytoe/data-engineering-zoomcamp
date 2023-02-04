FROM prefecthq/prefect:2.7.11-python3.10

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
# RUN mkdir -p /opt/prefect/data/yellow
# RUN mv /opt/prefect/flows/data /opt/prefect/.