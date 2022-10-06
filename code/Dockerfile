FROM python:3.8-slim

ENV APP_HOME /app

# Deploy the code
COPY . ${APP_HOME}

# Install dependencies
WORKDIR ${APP_HOME}
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "main.py"]