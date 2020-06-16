FROM postgres:12

RUN apt-get update && apt-get install postgresql-12-wal2json