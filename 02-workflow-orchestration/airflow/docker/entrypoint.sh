#!/usr/bin/env bash

# Then initialize or upgrade the Airflow DB
airflow db init
# or airflow db upgrade

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Finally, exec the main command (webserver, scheduler, etc.)
exec airflow "$@"