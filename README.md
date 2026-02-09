# postnl-data-engineering-assessment
This is my PostNL data engineering assessment where I design and implement a scalable data pipeline that processes transport and delivery data to improve efficiency, emissions insight, and data quality.


## Prerequisites

This setup assumes an up-to-date instance of Docker Desktop with docker compose is installed. If not, please follow the installation instructions on the [offical Docker website](https://docs.docker.com/get-started/get-docker/).

> Please make sure you can run commands with `root` privileges on your machine!

## Getting Started

### Clone the repository and cd into the folder

`clone` the repository:

```bash
git clone https://github.com/StefanNieuwenhuis/postnl-data-engineering-assessment.git
```

OR

Download and unzip the repository ZIP-file: https://github.com/StefanNieuwenhuis/postnl-data-engineering-assessment/archive/refs/heads/main.zip

### CD into the directory

```bash
cd postnl-data-engineering-assessment
```

### Initialize .env

```bash
mv .env.example .env
```

#### Optional
Open `.env` and edit values like `MINIO_ROOT_USER` or `MINIO_ROOT_PASSWORD`

### Run docker-compose.yaml

```bash
docker compose up -d
```

#### Successful completion

![Docker Compose has completed](./assets/DockerComposeComplete.png)

Check out the MinIO UI at: http://localhost:9000

RAW data should be _"ingested"_ in the `landing` bucket at: http://localhost:9001/browser/landing/sources 