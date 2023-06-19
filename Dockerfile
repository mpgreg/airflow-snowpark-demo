# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:8.5.0-base

PYENV 3.9 snowpark requirements-snowpark.txt

#Seed the base 3.9 python with snowpark packages for virtualenv operator
COPY requirements-snowpark.txt /tmp
RUN python3.9 -m pip install -r /tmp/requirements-snowpark.txt