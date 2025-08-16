# ./dbt/Dockerfile
FROM python:3.11-slim
RUN pip install --no-cache-dir "dbt-duckdb==1.8.2"
WORKDIR /usr/app
# keep the container running; you’ll exec into it to run dbt commands
ENTRYPOINT ["tail","-f","/dev/null"]
