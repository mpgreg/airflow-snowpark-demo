# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:8.5.0-base

COPY include/dist/astro_provider_snowpark-0.0.1a1-py3-none-any.whl /tmp

PYENV 3.9 snowpark requirements-snowpark.txt

#Seed the base 3.9 python with snowpark packages for virtualenv operator
COPY requirements-snowpark.txt /tmp
RUN python3.9 -m pip install -r /tmp/requirements-snowpark.txt