import os
import sys
import pandas as pd
import pytest

# Add parent directory to path to import validate_data_quality
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from validate_data_quality import (
    validate_column_list,
    validate_not_null,
    validate_column_type,
    validate_values_in_set,
    validate_unique
)

# Sample data for testing
@pytest.fixture
def sample_contracts_df():
    return pd.DataFrame({
        'contract_id': ['CONT-001', 'CONT-002', 'CONT-003', 'CONT-004', 'CONT-005'],
        'contract_name': ['Office Renovation', 'IT Infrastructure Upgrade', 'Marketing Campaign', 'Software Development', 'Consulting Services'],
        'client_name': ['Acme Corp', 'TechSolutions Inc', 'Global Marketing', 'Innovative Systems', 'Business Advisors'],
        'start_date': ['2023-01-01', '2023-02-15', '2023-03-10', '2023-04-01', '2023-05-15'],
        'end_date': ['2023-12-31', '2023-08-15', '2023-06-30', '2024-03-31', '2023-11-15'],
        'contract_value': [150000, 75000, 45000, 250000, 60000],
        'contract_status': ['executed', 'approved', 'pending', 'draft', 'sent']
    })

@pytest.fixture
def sample_budgets_df():
    return pd.DataFrame({
        'budget_id': ['BUD-001', 'BUD-002', 'BUD-003', 'BUD-004', 'BUD-005'],
        'contract_id': ['CONT-001', 'CONT-001', 'CONT-001', 'CONT-002', 'CONT-002'],
        'budget_name': ['Renovation Materials', 'Labor Costs', 'Permits and Fees', 'Hardware', 'Software Licenses'],
        'budget_amount': [80000, 60000, 10000, 45000, 20000],
        'fiscal_year': [2023, 2023, 2023, 2023, 2023],
        'department': ['Facilities', 'Facilities', 'Legal', 'IT', 'IT']
    })

# Test validate_column_list function
def test_validate_column_list_success(sample_contracts_df):
    expected_columns = ['contract_id', 'contract_name', 'client_name', 'start_date', 'end_date', 'contract_value', 'contract_status']
    assert validate_column_list(sample_contracts_df, expected_columns) == True

def test_validate_column_list_failure(sample_contracts_df):
    wrong_columns = ['id', 'name', 'client', 'start', 'end', 'value', 'status']
    assert validate_column_list(sample_contracts_df, wrong_columns) == False

# Test validate_not_null function
def test_validate_not_null_success(sample_contracts_df):
    assert validate_not_null(sample_contracts_df, 'contract_id') == True

def test_validate_not_null_failure(sample_contracts_df):
    # Create a dataframe with null values
    df_with_nulls = sample_contracts_df.copy()
    df_with_nulls.loc[0, 'contract_id'] = None
    assert validate_not_null(df_with_nulls, 'contract_id') == False

# Test validate_column_type function
def test_validate_column_type_success(sample_contracts_df):
    assert validate_column_type(sample_contracts_df, 'contract_value', 'NUMERIC') == True

def test_validate_column_type_failure(sample_contracts_df):
    assert validate_column_type(sample_contracts_df, 'contract_id', 'NUMERIC') == False

# Test validate_values_in_set function
def test_validate_values_in_set_success(sample_contracts_df):
    valid_statuses = ['executed', 'sent', 'draft', 'approved', 'pending']
    assert validate_values_in_set(sample_contracts_df, 'contract_status', valid_statuses) == True

def test_validate_values_in_set_failure(sample_contracts_df):
    invalid_statuses = ['active', 'inactive']
    assert validate_values_in_set(sample_contracts_df, 'contract_status', invalid_statuses) == False

# Test validate_unique function
def test_validate_unique_success(sample_contracts_df):
    assert validate_unique(sample_contracts_df, 'contract_id') == True

def test_validate_unique_failure(sample_budgets_df):
    assert validate_unique(sample_budgets_df, 'contract_id') == False 