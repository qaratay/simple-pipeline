# To run this local airflow instance
- clone this repo
- create .env -> copy the contents of .env.example into .env -> fill placeholders with your own values
- you will also need to place your service account key for google cloud console (name it `gcp-key.json`) into the root dir, it will be then mounted to /opt/airflow dir inside the containers
- or it maybe pasted as text in connection creation, either way it should work, but you will need to change dag code accordingly
- run `docker compose up -d` (while being inside the directory)

prereqs are to create bigquery instance