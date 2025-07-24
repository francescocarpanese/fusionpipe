import pytest
from unittest.mock import Mock, patch, MagicMock
from fusionpipe.utils import runner_utils


class TestPipelineHelperFunctions:
    """Test the new helper functions extracted from run_pipeline"""

    def test_validate_pipeline_inputs_valid(self):
        """Test validation with valid inputs"""
        # Should not raise any exceptions
        runner_utils._validate_pipeline_inputs(5, 300)
        runner_utils._validate_pipeline_inputs(None, None)
        runner_utils._validate_pipeline_inputs(1, 1)

    def test_validate_pipeline_inputs_invalid_concurrent_nodes(self):
        """Test validation with invalid max_concurrent_nodes"""
        with pytest.raises(ValueError, match="max_concurrent_nodes must be positive"):
            runner_utils._validate_pipeline_inputs(0, 300)
        
        with pytest.raises(ValueError, match="max_concurrent_nodes must be positive"):
            runner_utils._validate_pipeline_inputs(-1, 300)

    def test_validate_pipeline_inputs_invalid_timeout(self):
        """Test validation with invalid timeout"""
        with pytest.raises(ValueError, match="timeout must be positive"):
            runner_utils._validate_pipeline_inputs(5, 0)
        
        with pytest.raises(ValueError, match="timeout must be positive"):
            runner_utils._validate_pipeline_inputs(5, -1)

    def test_initialize_pipeline_state_basic(self):
        """Test basic pipeline state initialization"""
        mock_cur = Mock()
        
        with patch('fusionpipe.utils.db_utils.check_if_pipeline_exists', return_value=True), \
             patch('fusionpipe.utils.db_utils.get_all_nodes_from_pip_id', return_value=['node1', 'node2', 'node3']):
            
            all_nodes, excluded_nodes = runner_utils._initialize_pipeline_state(mock_cur, 'pipeline1')
            
            assert all_nodes == {'node1', 'node2', 'node3'}
            assert excluded_nodes == set()

    def test_initialize_pipeline_state_nonexistent_pipeline(self):
        """Test pipeline state initialization with non-existent pipeline"""
        mock_cur = Mock()
        
        with patch('fusionpipe.utils.db_utils.check_if_pipeline_exists', return_value=False):
            with pytest.raises(ValueError, match="Pipeline pipeline1 does not exist"):
                runner_utils._initialize_pipeline_state(mock_cur, 'pipeline1')

    def test_initialize_pipeline_state_with_last_node(self):
        """Test pipeline state initialization with last_node_id"""
        mock_cur = Mock()
        
        with patch('fusionpipe.utils.db_utils.check_if_pipeline_exists', return_value=True), \
             patch('fusionpipe.utils.db_utils.get_all_nodes_from_pip_id', return_value=['node1', 'node2', 'node3']), \
             patch('fusionpipe.utils.pip_utils.get_all_children_nodes', return_value=['node3']):
            
            all_nodes, excluded_nodes = runner_utils._initialize_pipeline_state(mock_cur, 'pipeline1', 'node2')
            
            assert all_nodes == {'node1', 'node2'}
            assert excluded_nodes == {'node3'}

    def test_initialize_pipeline_state_invalid_last_node(self):
        """Test pipeline state initialization with invalid last_node_id"""
        mock_cur = Mock()
        
        with patch('fusionpipe.utils.db_utils.check_if_pipeline_exists', return_value=True), \
             patch('fusionpipe.utils.db_utils.get_all_nodes_from_pip_id', return_value=['node1', 'node2']):
            
            with pytest.raises(ValueError, match="last_node_id node3 is not in pipeline pipeline1"):
                runner_utils._initialize_pipeline_state(mock_cur, 'pipeline1', 'node3')

    def test_update_node_status_from_database(self):
        """Test updating node status from database"""
        mock_cur = Mock()
        
        # Mock database status responses
        status_map = {'node1': 'completed', 'node2': 'failed', 'node3': 'running'}
        mock_cur.execute.return_value = None
        
        with patch('fusionpipe.utils.db_utils.get_node_status', side_effect=lambda cur, node_id: status_map[node_id]):
            all_nodes = {'node1', 'node2', 'node3', 'node4'}
            running_nodes = {'node3', 'node4'}  # node3 and node4 are already running
            completed_nodes = set()
            failed_nodes = set()
            
            runner_utils._update_node_status_from_database(
                mock_cur, all_nodes, running_nodes, completed_nodes, failed_nodes
            )
            
            # Only node1 and node2 should be updated (node3, node4 are already in running_nodes)
            assert completed_nodes == {'node1'}
            assert failed_nodes == {'node2'}

    def test_find_runnable_nodes_basic(self):
        """Test finding runnable nodes without concurrency limit"""
        mock_cur = Mock()
        
        with patch('fusionpipe.utils.pip_utils.can_node_run', side_effect=lambda cur, node_id: node_id in ['node1', 'node3']):
            all_nodes = {'node1', 'node2', 'node3', 'node4'}
            running_nodes = {'node2'}
            completed_nodes = {'node4'}
            failed_nodes = set()
            
            runnable_nodes = runner_utils._find_runnable_nodes(
                mock_cur, all_nodes, running_nodes, completed_nodes, failed_nodes
            )
            
            # node1 and node3 can run, but only node1 and node3 are not already processed
            assert set(runnable_nodes) == {'node1', 'node3'}

    def test_find_runnable_nodes_with_concurrency_limit(self):
        """Test finding runnable nodes with concurrency limit"""
        mock_cur = Mock()
        
        with patch('fusionpipe.utils.pip_utils.can_node_run', return_value=True):
            all_nodes = {'node1', 'node2', 'node3', 'node4'}
            running_nodes = {'node3'}  # 1 node already running
            completed_nodes = set()
            failed_nodes = set()
            max_concurrent_nodes = 2
            
            runnable_nodes = runner_utils._find_runnable_nodes(
                mock_cur, all_nodes, running_nodes, completed_nodes, failed_nodes, max_concurrent_nodes
            )
            
            # Should only return 1 node (2 max - 1 running = 1 available slot)
            assert len(runnable_nodes) == 1
            assert runnable_nodes[0] in ['node1', 'node2', 'node4']

    def test_find_runnable_nodes_exception_handling(self):
        """Test finding runnable nodes when pip_utils.can_node_run raises exception"""
        mock_cur = Mock()
        
        with patch('fusionpipe.utils.pip_utils.can_node_run', side_effect=Exception("Database error")):
            all_nodes = {'node1', 'node2'}
            running_nodes = set()
            completed_nodes = set()
            failed_nodes = set()
            
            runnable_nodes = runner_utils._find_runnable_nodes(
                mock_cur, all_nodes, running_nodes, completed_nodes, failed_nodes, debug=True
            )
            
            # Should return empty list when exception occurs
            assert runnable_nodes == []

    def test_check_execution_complete_all_processed(self):
        """Test execution complete when all nodes are processed"""
        all_nodes = {'node1', 'node2', 'node3'}
        completed_nodes = {'node1', 'node2'}
        failed_nodes = {'node3'}
        runnable_nodes = []
        running_nodes = set()
        
        result = runner_utils._check_execution_complete(
            all_nodes, completed_nodes, failed_nodes, runnable_nodes, running_nodes, debug=True
        )
        
        assert result is True

    def test_check_execution_complete_no_runnable_no_running(self):
        """Test execution complete when no runnable nodes and none running"""
        all_nodes = {'node1', 'node2', 'node3'}
        completed_nodes = {'node1'}
        failed_nodes = set()
        runnable_nodes = []
        running_nodes = set()
        
        result = runner_utils._check_execution_complete(
            all_nodes, completed_nodes, failed_nodes, runnable_nodes, running_nodes, debug=True
        )
        
        assert result is True

    def test_check_execution_complete_not_complete(self):
        """Test execution not complete when nodes are still runnable or running"""
        all_nodes = {'node1', 'node2', 'node3'}
        completed_nodes = {'node1'}
        failed_nodes = set()
        runnable_nodes = ['node2']
        running_nodes = set()
        
        result = runner_utils._check_execution_complete(
            all_nodes, completed_nodes, failed_nodes, runnable_nodes, running_nodes
        )
        
        assert result is False

    def test_start_runnable_nodes_success(self):
        """Test starting runnable nodes successfully"""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_proc = Mock()
        
        with patch('fusionpipe.utils.runner_utils.submit_run_node', return_value=mock_proc):
            runnable_nodes = ['node1', 'node2']
            running_nodes = set()
            failed_nodes = set()
            running_node_procs = {}
            
            runner_utils._start_runnable_nodes(
                mock_conn, mock_cur, runnable_nodes, running_nodes, failed_nodes, running_node_procs, debug=True
            )
            
            assert running_nodes == {'node1', 'node2'}
            assert running_node_procs == {'node1': mock_proc, 'node2': mock_proc}
            assert len(failed_nodes) == 0

    def test_start_runnable_nodes_with_failure(self):
        """Test starting runnable nodes with some failures"""
        mock_conn = Mock()
        mock_cur = Mock()
        
        def submit_side_effect(conn, node_id):
            if node_id == 'node2':
                raise RuntimeError("Node failed to start")
            return Mock()
        
        with patch('fusionpipe.utils.runner_utils.submit_run_node', side_effect=submit_side_effect), \
             patch('fusionpipe.utils.db_utils.update_node_status') as mock_update:
            
            runnable_nodes = ['node1', 'node2']
            running_nodes = set()
            failed_nodes = set()
            running_node_procs = {}
            
            runner_utils._start_runnable_nodes(
                mock_conn, mock_cur, runnable_nodes, running_nodes, failed_nodes, running_node_procs, debug=True
            )
            
            assert 'node1' in running_nodes
            assert 'node2' in failed_nodes
            assert 'node1' in running_node_procs
            assert 'node2' not in running_node_procs
            
            # Verify failed node status was updated
            mock_update.assert_called_with(mock_cur, 'node2', 'failed')

    def test_create_execution_summary(self):
        """Test creation of execution summary"""
        completed_nodes = {'node1', 'node2'}
        failed_nodes = {'node3'}
        excluded_nodes = {'node4', 'node5'}
        all_nodes = {'node1', 'node2', 'node3'}
        execution_time = 123.45
        
        summary = runner_utils._create_execution_summary(
            completed_nodes, failed_nodes, excluded_nodes, all_nodes, execution_time
        )
        
        expected = {
            "status": "failed",  # Because there are failed nodes
            "completed": 2,
            "failed": 1,
            "skipped": 2,
            "total": 5,  # 3 + 2 excluded
            "execution_time": 123.45
        }
        
        assert summary == expected

    def test_create_execution_summary_all_completed(self):
        """Test creation of execution summary with all nodes completed"""
        completed_nodes = {'node1', 'node2', 'node3'}
        failed_nodes = set()
        excluded_nodes = set()
        all_nodes = {'node1', 'node2', 'node3'}
        execution_time = 67.89
        
        summary = runner_utils._create_execution_summary(
            completed_nodes, failed_nodes, excluded_nodes, all_nodes, execution_time
        )
        
        expected = {
            "status": "completed",  # No failed nodes
            "completed": 3,
            "failed": 0,
            "skipped": 0,
            "total": 3,
            "execution_time": 67.89
        }
        
        assert summary == expected

    def test_handle_timeout_cleanup(self):
        """Test timeout cleanup functionality"""
        mock_conn = Mock()
        running_nodes = {'node1', 'node2'}
        failed_nodes = set()
        running_node_procs = {'node1': Mock(), 'node2': Mock()}
        timeout = 300
        
        with patch('fusionpipe.utils.runner_utils.kill_running_process') as mock_kill:
            with pytest.raises(TimeoutError, match="Pipeline execution exceeded timeout of 300 seconds"):
                runner_utils._handle_timeout_cleanup(
                    mock_conn, running_nodes, failed_nodes, running_node_procs, timeout, debug=True
                )
            
            # Verify all running nodes were killed
            assert mock_kill.call_count == 2
            assert failed_nodes == {'node1', 'node2'}
            assert running_nodes == set()
            assert running_node_procs == {}

    def test_handle_interrupt_cleanup(self):
        """Test interrupt cleanup functionality"""
        mock_conn = Mock()
        running_nodes = {'node1', 'node2'}
        failed_nodes = set()
        running_node_procs = {'node1': Mock(), 'node2': Mock()}
        
        with patch('fusionpipe.utils.runner_utils.kill_running_process') as mock_kill:
            runner_utils._handle_interrupt_cleanup(
                mock_conn, running_nodes, failed_nodes, running_node_procs, debug=True
            )
            
            # Verify all running nodes were killed
            assert mock_kill.call_count == 2
            assert failed_nodes == {'node1', 'node2'}
            assert running_nodes == set()
            assert running_node_procs == {}

    def test_update_running_nodes_status_success(self):
        """Test updating running nodes status successfully"""
        mock_conn = Mock()
        mock_cur = Mock()
        running_nodes = {'node1', 'node2'}
        completed_nodes = set()
        failed_nodes = set()
        
        # Create specific mock objects for each node to track them
        proc1 = Mock(name='proc1')
        proc2 = Mock(name='proc2')
        running_node_procs = {'node1': proc1, 'node2': proc2}
        
        # Create a side effect function that returns different statuses for different procs
        def status_side_effect(proc):
            if proc is proc1:  # Use 'is' for identity comparison
                return 'completed'
            elif proc is proc2:  # Use 'is' for identity comparison
                return 'running'
            return 'failed'
        
        with patch('fusionpipe.utils.runner_utils.check_proc_status', side_effect=status_side_effect) as mock_check_status, \
             patch('fusionpipe.utils.db_utils.update_node_status') as mock_update:
            
            # Ensure commit doesn't raise an exception
            mock_conn.commit.return_value = None
            
            runner_utils._update_running_nodes_status(
                mock_conn, mock_cur, running_nodes, completed_nodes, failed_nodes, running_node_procs, debug=True
            )
            
            # Debug: Print what actually happened
            print(f"Completed nodes: {completed_nodes}")
            print(f"Failed nodes: {failed_nodes}")
            print(f"Running nodes: {running_nodes}")
            print(f"Running node procs: {running_node_procs}")
            print(f"check_proc_status call count: {mock_check_status.call_count}")
            print(f"update_node_status call count: {mock_update.call_count}")
            
            # node1 should be moved to completed, node2 should remain running
            assert completed_nodes == {'node1'}
            assert failed_nodes == set()
            assert running_nodes == {'node2'}
            assert 'node1' not in running_node_procs
            assert 'node2' in running_node_procs
            
            # Verify database updates were called correctly
            assert mock_update.call_count == 2
            mock_update.assert_any_call(mock_cur, 'node1', 'completed')
            mock_update.assert_any_call(mock_cur, 'node2', 'running')

    def test_update_running_nodes_status_with_exception(self):
        """Test updating running nodes status when exception occurs"""
        mock_conn = Mock()
        mock_cur = Mock()
        running_nodes = {'node1'}
        completed_nodes = set()
        failed_nodes = set()
        running_node_procs = {'node1': Mock()}
        
        with patch('fusionpipe.utils.runner_utils.check_proc_status', side_effect=Exception("Status check failed")), \
             patch('fusionpipe.utils.db_utils.update_node_status') as mock_update:
            
            runner_utils._update_running_nodes_status(
                mock_conn, mock_cur, running_nodes, completed_nodes, failed_nodes, running_node_procs, debug=True
            )
            
            # node1 should be moved to failed due to exception
            assert completed_nodes == set()
            assert failed_nodes == {'node1'}
            assert running_nodes == set()
            assert 'node1' not in running_node_procs
            
            # Verify failed status was set
            mock_update.assert_called_with(mock_cur, 'node1', 'failed')
