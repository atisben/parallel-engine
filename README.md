# Parallel engine

Parallel engine is a simplistic Virtual machine launcher for data science jobs
It allows to parallelize docker images over several machines



## MVP Architecture
![Architecure](./docs/architecture.png)

## Google cloud authentication

Run the following command to connect to GCP. If you are running the command on a local machine, make sure you've installed the [google cloud CLI](https://cloud.google.com/sdk/docs/install)

```sh
    gcloud auth application-default login
    gcloud auth login
```

Follow the instructions and login to your GCP project

## Set up the environment variables

Make sure you've created a `.config.yaml` file in the root directory
This yaml file shoud at least contain the folowing values
set environment variables
```yaml
PROJECT_ID: your project
ZONE: your zone
MACHINE_TYPE: machine type
VM_QUOTAS: max number of machines on the project
```

## install all the dependencies

```sh
pip install -r requirements.txt
```

## Cloud build push docker image

Cloud Build is used to push the docker image in Google Container Registry.
Run the following command to push the docker image to gcr

```sh
gcloud builds submit \
--project ${PROJECT_ID}
```

## Local debbuging

```sh
docker-credential-gcr configure-docker 
docker run ${IMAGE_URI} --project ${PROJECT_ID} --dataset ${DATASET}
```
