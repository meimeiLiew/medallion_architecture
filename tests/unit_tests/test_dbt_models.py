import os
import unittest
import yaml
import glob
import re
from pathlib import Path

class TestDbtModels(unittest.TestCase):
    """Test class for validating dbt models structure and configuration."""
    
    def setUp(self):
        """Set up test environment."""
        self.project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
        self.dbt_dir = os.path.join(self.project_dir, 'silver_layer/transformations')
        self.models_dir = os.path.join(self.dbt_dir, 'models')
        
        # Load dbt_project.yml
        with open(os.path.join(self.dbt_dir, 'dbt_project.yml'), 'r') as f:
            self.dbt_project = yaml.safe_load(f)
    
    def test_dbt_project_structure(self):
        """Test that the dbt project structure is valid."""
        # Check that dbt_project.yml exists
        self.assertTrue(os.path.exists(os.path.join(self.dbt_dir, 'dbt_project.yml')))
        
        # Check that models directory exists
        self.assertTrue(os.path.exists(self.models_dir))
        
        # Check that profiles directory exists
        self.assertTrue(os.path.exists(os.path.join(self.dbt_dir, 'profiles')))
        
        # Check that silver and gold directories exist
        self.assertTrue(os.path.exists(os.path.join(self.models_dir, 'silver')))
        self.assertTrue(os.path.exists(os.path.join(self.models_dir, 'gold')))
    
    def test_model_naming_convention(self):
        """Test that model files follow naming conventions."""
        # All SQL files should follow snake_case naming convention
        sql_files = glob.glob(os.path.join(self.models_dir, '**/*.sql'), recursive=True)
        for sql_file in sql_files:
            filename = os.path.basename(sql_file)
            self.assertTrue(re.match(r'^[a-z][a-z0-9_]*\.sql$', filename), 
                           f"File {filename} does not follow snake_case naming convention")
    
    def test_model_configurations(self):
        """Test that model configurations are valid."""
        # Check silver models
        silver_models = glob.glob(os.path.join(self.models_dir, 'silver/*.sql'))
        for model in silver_models:
            with open(model, 'r') as f:
                content = f.read()
                # Check that silver models have the correct materialization
                self.assertIn("materialized='table'", content, 
                             f"Silver model {os.path.basename(model)} should be materialized as a table")
                self.assertIn("schema='silver'", content, 
                             f"Silver model {os.path.basename(model)} should be in the silver schema")
        
        # Check gold models
        gold_models = glob.glob(os.path.join(self.models_dir, 'gold/*.sql'))
        for model in gold_models:
            with open(model, 'r') as f:
                content = f.read()
                # Check that gold models have the correct materialization
                self.assertIn("materialized='table'", content, 
                             f"Gold model {os.path.basename(model)} should be materialized as a table")
                self.assertIn("schema='gold'", content, 
                             f"Gold model {os.path.basename(model)} should be in the gold schema")
    
    def test_schema_yml_files(self):
        """Test that schema.yml files are valid."""
        # Check that schema.yml files exist in silver and gold directories
        self.assertTrue(os.path.exists(os.path.join(self.models_dir, 'silver/schema.yml')))
        self.assertTrue(os.path.exists(os.path.join(self.models_dir, 'gold/schema.yml')))
        
        # Load schema.yml files
        with open(os.path.join(self.models_dir, 'silver/schema.yml'), 'r') as f:
            silver_schema = yaml.safe_load(f)
        
        with open(os.path.join(self.models_dir, 'gold/schema.yml'), 'r') as f:
            gold_schema = yaml.safe_load(f)
        
        # Check that schema.yml files have the correct version
        self.assertEqual(silver_schema.get('version'), 2, "Silver schema.yml should have version 2")
        self.assertEqual(gold_schema.get('version'), 2, "Gold schema.yml should have version 2")
        
        # Check that schema.yml files have models defined
        self.assertTrue(len(silver_schema.get('models', [])) > 0, "Silver schema.yml should have models defined")
        self.assertTrue(len(gold_schema.get('models', [])) > 0, "Gold schema.yml should have models defined")
        
        # Check that each model in schema.yml has a corresponding SQL file
        for model in silver_schema.get('models', []):
            model_name = model.get('name')
            self.assertTrue(os.path.exists(os.path.join(self.models_dir, f'silver/{model_name}.sql')), 
                           f"Model {model_name} defined in silver schema.yml but SQL file not found")
        
        for model in gold_schema.get('models', []):
            model_name = model.get('name')
            self.assertTrue(os.path.exists(os.path.join(self.models_dir, f'gold/{model_name}.sql')), 
                           f"Model {model_name} defined in gold schema.yml but SQL file not found")
    
    def test_sources_yml_file(self):
        """Test that sources.yml file is valid."""
        # Check that sources.yml file exists
        self.assertTrue(os.path.exists(os.path.join(self.models_dir, 'sources.yml')))
        
        # Load sources.yml file
        with open(os.path.join(self.models_dir, 'sources.yml'), 'r') as f:
            sources = yaml.safe_load(f)
        
        # Check that sources.yml file has the correct version
        self.assertEqual(sources.get('version'), 2, "sources.yml should have version 2")
        
        # Check that sources.yml file has sources defined
        self.assertTrue(len(sources.get('sources', [])) > 0, "sources.yml should have sources defined")
        
        # Check that bronze source is defined
        bronze_source = next((s for s in sources.get('sources', []) if s.get('name') == 'bronze'), None)
        self.assertIsNotNone(bronze_source, "Bronze source should be defined in sources.yml")
        
        # Check that bronze source has tables defined
        self.assertTrue(len(bronze_source.get('tables', [])) > 0, "Bronze source should have tables defined")
        
        # Check that required tables are defined
        table_names = [t.get('name') for t in bronze_source.get('tables', [])]
        self.assertIn('contracts', table_names, "contracts table should be defined in bronze source")
        self.assertIn('budgets', table_names, "budgets table should be defined in bronze source")
        self.assertIn('change_orders', table_names, "change_orders table should be defined in bronze source")

if __name__ == '__main__':
    unittest.main() 