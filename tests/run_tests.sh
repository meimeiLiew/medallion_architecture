#!/bin/bash
set -e

echo "Running all tests for medallion architecture..."

# Set environment variables for testing
export USE_SAMPLE_FILES=True

# Create directories if they don't exist
mkdir -p ../bronze/data
mkdir -p ../silver/data

# Ensure sample data files exist
if [ ! -f "../bronze/data/sample_contracts.csv" ]; then
  echo "Creating sample contracts data..."
  echo "contract_id,contract_name,client_name,start_date,end_date,contract_value,contract_status
CONT-001,Office Renovation,Acme Corp,2023-01-01,2023-12-31,150000,executed
CONT-002,IT Infrastructure Upgrade,TechSolutions Inc,2023-02-15,2023-08-15,75000,approved
CONT-003,Marketing Campaign,Global Marketing,2023-03-10,2023-06-30,45000,pending
CONT-004,Software Development,Innovative Systems,2023-04-01,2024-03-31,250000,draft
CONT-005,Consulting Services,Business Advisors,2023-05-15,2023-11-15,60000,sent" > ../bronze/data/sample_contracts.csv
fi

if [ ! -f "../bronze/data/sample_budgets.csv" ]; then
  echo "Creating sample budgets data..."
  echo "budget_id,contract_id,budget_name,budget_amount,fiscal_year,department
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
BUD-014,CONT-005,Implementation Support,20000,2023,Operations" > ../bronze/data/sample_budgets.csv
fi

# Run unit tests
echo "Running unit tests..."
pytest unit_tests/ -v

# Run data quality validation
echo "Running data quality validation..."
python validate_data_quality.py

# Run integration tests
echo "Running integration tests..."
pytest integration_tests/ -v

echo "All tests completed!" 