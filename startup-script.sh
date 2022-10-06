#! /bin/bash
sudo echo "> machine is running fine"

# Docker test
sudo apt update
sudo apt install --yes apt-transport-https ca-certificates curl gnupg2 software-properties-common
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
sudo apt update
sudo apt install --yes docker-ce

# Get access token
docker login -u oauth2accesstoken -p "$(gcloud auth print-access-token)" https://gcr.io
# Run docker container
sudo docker run gcr.io/parallel-engine/job-runner:latest --project {project} --dataset {dataset} --var {var}

# Delete the compute engine
gcloud compute instances delete {instance_name} --zone {zone} -y