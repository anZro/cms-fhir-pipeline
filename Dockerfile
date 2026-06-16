FROM astrocrpublic.azurecr.io/runtime:3.2-5
COPY .env /usr/local/airflow/.env
COPY service-account.json /usr/local/airflow/service-account.json
