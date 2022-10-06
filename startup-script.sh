#! /bin/bash
sudo echo "> machine is running fine"
sudo docker run gcr.io/parallel-engine/job-runner:latest --project parallel-engine --dataset test --var {var}

