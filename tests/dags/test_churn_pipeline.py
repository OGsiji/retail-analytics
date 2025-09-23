"""
Tests for churn prediction pipeline DAG
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
import pandas as pd


class TestChurnPipelineDAG:
    """Test suite for the churn prediction pipeline DAG"""
    
    def setup_method(self):
        """Setup test environment"""
        self.dagbag = DagBag(dag_folder='dags', include_examples=False)
        self.dag_id = 'churn_prediction_pipeline'
        
    def test_dag_loaded(self):
        """Test that the DAG is properly loaded"""
        dag = self.dagbag.get_dag(self.dag_id)
        assert dag is not None
        assert dag.dag_id == self.dag_id
        
    def test_dag_structure(self):
        """Test DAG structure and task dependencies"""
        dag = self.dagbag.get_dag(self.dag_id)
        
        # Check key tasks exist
        expected_tasks = [
            'create_database_schema',
            'load_transaction_dump',
            'load_users_csv', 
            'load_activities_csv',
            'validate_raw_data',
            'validate_final_features',
            'refresh_api_cache'
        ]
        
        actual_tasks = [task.task_id for task in dag.tasks]
        
        for expected_task in expected_tasks:
            assert expected_task in actual_tasks, f"Task {expected_task} not found"
            
    def test_dag_configuration(self):
        """Test DAG configuration parameters"""
        dag = self.dagbag.get_dag(self.dag_id)
        
        # Check DAG properties
        assert dag.schedule_interval == '@daily'
        assert dag.catchup == False
        assert dag.max_active_runs == 1
        assert 'churn' in dag.tags
        assert 'ml' in dag.tags
        assert 'etl' in dag.tags
        
    def test_no_import_errors(self):
        """Test that there are no import errors"""
        assert len(self.dagbag.import_errors) == 0, f"Import errors: {self.dagbag.import_errors}"
        
    @patch('dags.churn_pipeline.PostgresHook')
    def test_load_users_csv_task(self, mock_postgres_hook):
        """Test the load_users_csv task"""
        # Mock the database connection
        mock_hook = MagicMock()
        mock_postgres_hook.return_value = mock_hook
        
        # Mock pandas read_csv
        with patch('pandas.read_csv') as mock_read_csv:
            mock_df = pd.DataFrame({
                'user_id': [1, 2, 3],
                'signup_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
                'region': ['lagos', 'abuja', 'kano'],
                'channel': ['organic', 'paid', 'referral']
            })
            mock_read_csv.return_value = mock_df
            
            dag = self.dagbag.get_dag(self.dag_id)
            task = dag.get_task('load_users_csv')
            
            # Execute task
            ti = TaskInstance(task=task, execution_date=datetime.now())
            result = task.execute(ti.get_template_context())
            
            # Verify results
            assert result['users_loaded'] == 3
            mock_hook.run.assert_called()

    @patch('requests.post')
    def test_refresh_api_cache_task(self, mock_post):
        """Test the refresh_api_cache task"""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        dag = self.dagbag.get_dag(self.dag_id)
        task = dag.get_task('refresh_api_cache')
        
        ti = TaskInstance(task=task, execution_date=datetime.now())
        result = task.execute(ti.get_template_context())
        
        assert result['cache_refresh'] == 'success'