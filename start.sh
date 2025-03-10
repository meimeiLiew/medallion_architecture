#!/bin/bash
set -e

echo "Starting Medallion Architecture Data Pipeline..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker and Docker Compose first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Creating sample .env file..."
    cat > .env << EOF
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET_NAME=your-gcs-bucket-name
BIGQUERY_DATASET_SILVER=your_silver_dataset_name
BIGQUERY_DATASET_GOLD=your_gold_dataset_name
GCP_CREDENTIALS_PATH=path/to/your/credentials.json
DBT_CLOUD_JOB_ID=your-dbt-cloud-job-id
DBT_CLOUD_ACCOUNT_ID=your-dbt-cloud-account-id
EOF
    echo "Please update the .env file with your actual values."
fi

# Create necessary directories
mkdir -p bronze/data
mkdir -p silver/data
mkdir -p gold/data
mkdir -p tests/great_expectations/uncommitted/data_docs/local_site

# Create sample data files if they don't exist
if [ ! -f "bronze/data/sample_contracts.csv" ]; then
    echo "Creating sample contracts data..."
    cat > bronze/data/sample_contracts.csv << EOF
contract_id,contract_name,client_name,start_date,end_date,contract_value,contract_status
CONT-001,Office Renovation,Acme Corp,2023-01-01,2023-12-31,150000,executed
CONT-002,IT Infrastructure Upgrade,TechSolutions Inc,2023-02-15,2023-08-15,75000,approved
CONT-003,Marketing Campaign,Global Marketing,2023-03-10,2023-06-30,45000,pending
CONT-004,Software Development,Innovative Systems,2023-04-01,2024-03-31,250000,draft
CONT-005,Consulting Services,Business Advisors,2023-05-15,2023-11-15,60000,sent
EOF
fi

if [ ! -f "bronze/data/sample_budgets.csv" ]; then
    echo "Creating sample budgets data..."
    cat > bronze/data/sample_budgets.csv << EOF
budget_id,contract_id,budget_name,budget_amount,fiscal_year,department
BUD-001,CONT-001,Renovation Materials,80000,2023,Facilities
BUD-002,CONT-001,Labor Costs,60000,2023,Facilities
BUD-003,CONT-001,Permits and Fees,10000,2023,Legal
BUD-004,CONT-002,Hardware,45000,2023,IT
BUD-005,CONT-002,Software Licenses,20000,2023,IT
BUD-006,CONT-002,Installation Services,10000,2023,IT
BUD-007,CONT-003,Digital Advertising,25000,2023,Marketing
BUD-008,CONT-003,Content Creation,15000,2023,Marketing
BUD-009,CONT-003,Analytics,5000,2023,Marketing
BUD-010,CONT-004,Development Team,150000,2023,Engineering
BUD-011,CONT-004,QA Testing,50000,2023,Engineering
BUD-012,CONT-004,Project Management,50000,2023,Engineering
BUD-013,CONT-005,Strategy Consulting,40000,2023,Executive
BUD-014,CONT-005,Implementation Support,20000,2023,Operations
EOF
fi

# Start the Docker containers
echo "Starting Docker containers..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10

# Check if services are running
if docker-compose ps | grep -q "Up"; then
    echo "Services are running!"
    echo "You can access Airflow at http://localhost:8080"
    echo "Username: admin"
    echo "Password: admin"
else
    echo "Error: Services failed to start. Check the logs with 'docker-compose logs'"
    exit 1
fi

# Run data quality validation
echo "Running data quality validation..."
docker-compose exec -T airflow-webserver /opt/airflow/docker/validate_data.sh

echo "Setup complete! Your medallion architecture is ready to use."
echo "For more information, see the README.md file." 