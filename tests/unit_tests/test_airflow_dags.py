import os
import unittest
import sys
from airflow.models import DagBag

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

class TestAirflowDags(unittest.TestCase):
    """Test class for validating Airflow DAGs."""
    
    def setUp(self):
        """Set up test environment."""
        self.project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
        self.dags_dir = os.path.join('/opt/airflow/dags')
        
        # Set Airflow home to the project directory
        os.environ['AIRFLOW_HOME'] = '/opt/airflow'
        
        # Load DAGs
        self.dagbag = DagBag(dag_folder=self.dags_dir, include_examples=False)
    
    def test_dag_loading(self):
        """Test that all DAGs can be loaded without errors."""
        # Check for import errors
        self.assertFalse(
            len(self.dagbag.import_errors),
            f"DAG import errors: {self.dagbag.import_errors}"
        )
    
    def test_medallion_pipeline_dag_structure(self):
        """Test the structure of the medallion_pipeline DAG."""
        # Check that the medallion_pipeline DAG exists
        dag_id = 'medallion_pipeline'
        self.assertIn(dag_id, self.dagbag.dags, f"DAG {dag_id} not found")
        
        # Get the DAG
        dag = self.dagbag.dags[dag_id]
        
        # Check that the DAG has the expected tasks
        expected_tasks = [
            'create_bronze_dataset',
            'create_silver_dataset',
            'create_gold_dataset',
            'upload_contracts_to_gcs',
            'upload_budgets_to_gcs',
            'upload_co_to_gcs',
            'load_contracts_to_bq',
            'load_budgets_to_bq',
            'load_co_to_bq',
            'run_dbt_models',
            'validate_silver_contracts',
            'validate_silver_budgets',
            'validate_silver_change_orders',
            'validate_gold_project_analytics'
        ]
        
        for task_id in expected_tasks:
            self.assertIn(task_id, dag.task_ids, f"Task {task_id} not found in DAG {dag_id}")
        
        # Check task dependencies
        # Bronze dataset creation should be upstream of upload tasks
        for task_id in ['upload_contracts_to_gcs', 'upload_budgets_to_gcs', 'upload_co_to_gcs']:
            self.assertIn(
                'create_bronze_dataset',
                [task.task_id for task in dag.get_task(task_id).upstream_list],
                f"create_bronze_dataset should be upstream of {task_id}"
            )
        
        # Upload tasks should be upstream of load tasks
        self.assertIn(
            'upload_contracts_to_gcs',
            [task.task_id for task in dag.get_task('load_contracts_to_bq').upstream_list],
            "upload_contracts_to_gcs should be upstream of load_contracts_to_bq"
        )
        
        self.assertIn(
            'upload_budgets_to_gcs',
            [task.task_id for task in dag.get_task('load_budgets_to_bq').upstream_list],
            "upload_budgets_to_gcs should be upstream of load_budgets_to_bq"
        )
        
        self.assertIn(
            'upload_co_to_gcs',
            [task.task_id for task in dag.get_task('load_co_to_bq').upstream_list],
            "upload_co_to_gcs should be upstream of load_co_to_bq"
        )
        
        # Load tasks should be upstream of run_dbt_models
        for task_id in ['load_contracts_to_bq', 'load_budgets_to_bq', 'load_co_to_bq']:
            self.assertIn(
                task_id,
                [task.task_id for task in dag.get_task('run_dbt_models').upstream_list],
                f"{task_id} should be upstream of run_dbt_models"
            )
        
        # Silver and gold dataset creation should be upstream of run_dbt_models
        for task_id in ['create_silver_dataset', 'create_gold_dataset']:
            self.assertIn(
                task_id,
                [task.task_id for task in dag.get_task('run_dbt_models').upstream_list],
                f"{task_id} should be upstream of run_dbt_models"
            )
        
        # run_dbt_models should be upstream of validation tasks
        for task_id in ['validate_silver_contracts', 'validate_silver_budgets', 'validate_silver_change_orders', 'validate_gold_project_analytics']:
            self.assertIn(
                'run_dbt_models',
                [task.task_id for task in dag.get_task(task_id).upstream_list],
                f"run_dbt_models should be upstream of {task_id}"
            )
    
    def test_medallion_monitoring_dag_structure(self):
        """Test the structure of the medallion_monitoring DAG."""
        # Check that the medallion_monitoring DAG exists
        dag_id = 'medallion_monitoring'
        self.assertIn(dag_id, self.dagbag.dags, f"DAG {dag_id} not found")
        
        # Get the DAG
        dag = self.dagbag.dags[dag_id]
        
        # Check that the DAG has the expected tasks
        expected_tasks = [
            'check_data_freshness',
            'check_data_volume_trends',
            'generate_monitoring_report',
            'check_null_values'
        ]
        
        for task_id in expected_tasks:
            self.assertIn(task_id, dag.task_ids, f"Task {task_id} not found in DAG {dag_id}")
        
        # Check task dependencies
        # check_data_freshness and check_data_volume_trends should be upstream of generate_monitoring_report
        for task_id in ['check_data_freshness', 'check_data_volume_trends']:
            self.assertIn(
                task_id,
                [task.task_id for task in dag.get_task('generate_monitoring_report').upstream_list],
                f"{task_id} should be upstream of generate_monitoring_report"
            )

if __name__ == '__main__':
    unittest.main() 