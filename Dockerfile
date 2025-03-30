FROM apache/airflow:2.10.5 

# Set working directory to where Airflow expects DAGs and plugins
WORKDIR /app

COPY . .

# Install the requirements
RUN pip install -r requirements.txt
