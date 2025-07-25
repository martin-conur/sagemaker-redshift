import pytest
from unittest.mock import Mock, patch, MagicMock
import polars as pl
from redshift_utils import unload_redshift, copy_to_redshift, copy_s3_to_redshift


class TestUnloadRedshift:
    """Test cases for unload_redshift function"""
    
    def test_missing_credentials_raises_error(self):
        """Test that missing credentials raise ValueError"""
        with pytest.raises(ValueError, match="All credential parameters"):
            unload_redshift(
                query="SELECT * FROM table",
                destination="s3://bucket/path",
                db="",
                cluster_id="cluster",
                db_user="user",
                ds_role="role"
            )
    
    @patch('redshift_utils.boto3.Session')
    @patch('redshift_utils.s.get_session')
    def test_valid_unload_csv(self, mock_get_session, mock_boto_session):
        """Test valid CSV unload with all parameters"""
        # Mock the boto3 clients
        mock_redshift_client = Mock()
        mock_s3_client = Mock()
        mock_session_instance = Mock()
        mock_session_instance.client.side_effect = lambda service: {
            'redshift-data': mock_redshift_client,
            's3': mock_s3_client
        }[service]
        mock_boto_session.return_value = mock_session_instance
        
        # Mock execute_statement response
        mock_redshift_client.execute_statement.return_value = {"Id": "test-id-123"}
        
        # Mock describe_statement response (successful completion)
        mock_redshift_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "Duration": 1000000  # 1 second in microseconds
        }
        
        # Mock S3 list response
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "test.csv", "Size": 1024}]
        }
        
        # Execute function
        unload_redshift(
            query="SELECT * FROM test_table",
            destination="s3://test-bucket/test-path/",
            db="test_db",
            cluster_id="test-cluster",
            db_user="test-user",
            ds_role="arn:aws:iam::123456789012:role/test-role",
            verbose=0
        )
        
        # Verify execute_statement was called with correct parameters
        mock_redshift_client.execute_statement.assert_called_once()
        call_args = mock_redshift_client.execute_statement.call_args[1]
        assert call_args['Database'] == 'test_db'
        assert call_args['DbUser'] == 'test-user'
        assert call_args['ClusterIdentifier'] == 'test-cluster'
        assert "SELECT * FROM test_table" in call_args['Sql']
        assert "s3://test-bucket/test-path/" in call_args['Sql']
        assert "arn:aws:iam::123456789012:role/test-role" in call_args['Sql']
    
    @patch('redshift_utils.boto3.Session')
    @patch('redshift_utils.s.get_session')
    def test_unload_with_parquet_format(self, mock_get_session, mock_boto_session):
        """Test unload with parquet format"""
        # Setup mocks
        mock_redshift_client = Mock()
        mock_s3_client = Mock()
        mock_session_instance = Mock()
        mock_session_instance.client.side_effect = lambda service: {
            'redshift-data': mock_redshift_client,
            's3': mock_s3_client
        }[service]
        mock_boto_session.return_value = mock_session_instance
        
        mock_redshift_client.execute_statement.return_value = {"Id": "test-id"}
        mock_redshift_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "Duration": 1000000
        }
        mock_s3_client.list_objects_v2.return_value = {"Contents": [{"Key": "test.parquet", "Size": 2048}]}
        
        # Execute with parquet format
        unload_redshift(
            query="SELECT * FROM table",
            destination="s3://bucket/path/",
            db="db",
            cluster_id="cluster",
            db_user="user",
            ds_role="role",
            file_format="parquet",
            verbose=0
        )
        
        # Check that SQL contains parquet format
        sql = mock_redshift_client.execute_statement.call_args[1]['Sql']
        assert "format as parquet" in sql
        assert "HEADER" not in sql  # Header not supported for parquet
        assert "DELIMITER" not in sql  # Delimiter not supported for parquet
    
    def test_invalid_file_format_raises_error(self):
        """Test that invalid file format raises AssertionError"""
        with pytest.raises(AssertionError, match="file_format not valid"):
            unload_redshift(
                query="SELECT * FROM table",
                destination="s3://bucket/path",
                db="db",
                cluster_id="cluster",
                db_user="user",
                ds_role="role",
                file_format="invalid_format"
            )


class TestCopyToRedshift:
    """Test cases for copy_to_redshift function"""
    
    def test_missing_credentials_raises_error(self):
        """Test that missing credentials raise ValueError"""
        df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        
        with pytest.raises(ValueError, match="All credential parameters"):
            copy_to_redshift(
                df=df,
                table_name="test_table",
                schema="test_schema",
                s3_bucket="test-bucket",
                db="",  # Empty db
                cluster_id="cluster",
                db_user="user",
                ds_role="role"
            )
    
    @patch('redshift_utils.boto3.Session')
    @patch('redshift_utils.s.get_session')
    @patch('tempfile.NamedTemporaryFile')
    @patch('os.unlink')
    def test_valid_copy_operation(self, mock_unlink, mock_temp_file, mock_get_session, mock_boto_session):
        """Test valid copy operation with DataFrame"""
        # Setup DataFrame
        df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        
        # Mock temporary file
        mock_temp = Mock()
        mock_temp.name = "/tmp/test.csv"
        mock_temp_file.return_value.__enter__.return_value = mock_temp
        
        # Mock boto3 clients
        mock_redshift_client = Mock()
        mock_s3_client = Mock()
        mock_session_instance = Mock()
        mock_session_instance.client.side_effect = lambda service: {
            'redshift-data': mock_redshift_client,
            's3': mock_s3_client
        }[service]
        mock_boto_session.return_value = mock_session_instance
        
        # Mock responses
        mock_redshift_client.execute_statement.return_value = {"Id": "copy-id"}
        mock_redshift_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "Duration": 500000
        }
        
        # Execute function
        copy_to_redshift(
            df=df,
            table_name="test_table",
            schema="test_schema",
            s3_bucket="test-bucket",
            db="test_db",
            cluster_id="test-cluster",
            db_user="test-user",
            ds_role="arn:aws:iam::123456789012:role/test-role",
            verbose=0
        )
        
        # Verify S3 upload was called
        mock_s3_client.upload_file.assert_called_once()
        
        # Verify COPY command was executed
        copy_sql = mock_redshift_client.execute_statement.call_args[1]['Sql']
        assert "COPY test_schema.test_table" in copy_sql
        assert "FROM 's3://test-bucket/temp_loads/test_table_" in copy_sql
        assert "FORMAT AS CSV" in copy_sql
        assert "IGNOREHEADER 1" in copy_sql
    
    @patch('redshift_utils.boto3.Session')
    @patch('redshift_utils.s.get_session')
    @patch('tempfile.NamedTemporaryFile')
    @patch('os.unlink')
    def test_copy_with_truncate(self, mock_unlink, mock_temp_file, mock_get_session, mock_boto_session):
        """Test copy operation with truncate option"""
        df = pl.DataFrame({"col1": [1, 2, 3]})
        
        # Setup mocks
        mock_temp = Mock()
        mock_temp.name = "/tmp/test.csv"
        mock_temp_file.return_value.__enter__.return_value = mock_temp
        
        mock_redshift_client = Mock()
        mock_s3_client = Mock()
        mock_session_instance = Mock()
        mock_session_instance.client.side_effect = lambda service: {
            'redshift-data': mock_redshift_client,
            's3': mock_s3_client
        }[service]
        mock_boto_session.return_value = mock_session_instance
        
        # Mock responses for both truncate and copy operations
        mock_redshift_client.execute_statement.side_effect = [
            {"Id": "truncate-id"},  # First call for TRUNCATE
            {"Id": "copy-id"}       # Second call for COPY
        ]
        mock_redshift_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "Duration": 100000
        }
        
        # Execute with truncate
        copy_to_redshift(
            df=df,
            table_name="test_table",
            schema="test_schema",
            s3_bucket="test-bucket",
            db="db",
            cluster_id="cluster",
            db_user="user",
            ds_role="role",
            if_exists="truncate",
            verbose=0
        )
        
        # Verify truncate was called first
        first_sql = mock_redshift_client.execute_statement.call_args_list[0][1]['Sql']
        assert "TRUNCATE TABLE test_schema.test_table" in first_sql
        
        # Verify copy was called second
        second_sql = mock_redshift_client.execute_statement.call_args_list[1][1]['Sql']
        assert "COPY test_schema.test_table" in second_sql


class TestCopyS3ToRedshift:
    """Test cases for copy_s3_to_redshift function"""
    
    def test_missing_credentials_raises_error(self):
        """Test that missing credentials raise ValueError"""
        with pytest.raises(ValueError, match="All credential parameters"):
            copy_s3_to_redshift(
                s3_uri="s3://bucket/file.csv",
                table_name="test_table",
                schema="test_schema",
                db=None,  # None value
                cluster_id="cluster",
                db_user="user",
                ds_role="role"
            )
    
    @patch('redshift_utils.boto3.Session')
    @patch('redshift_utils.s.get_session')
    def test_copy_from_s3_csv(self, mock_get_session, mock_boto_session):
        """Test copying CSV file from S3"""
        # Mock boto3 clients
        mock_redshift_client = Mock()
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_redshift_client
        mock_boto_session.return_value = mock_session_instance
        
        # Mock responses
        mock_redshift_client.execute_statement.return_value = {"Id": "copy-id"}
        mock_redshift_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "Duration": 2000000
        }
        
        # Execute function
        copy_s3_to_redshift(
            s3_uri="s3://test-bucket/data/file.csv",
            table_name="test_table",
            schema="test_schema",
            db="test_db",
            cluster_id="test-cluster",
            db_user="test-user",
            ds_role="arn:aws:iam::123456789012:role/test-role",
            file_format="csv",
            verbose=0
        )
        
        # Verify COPY command
        sql = mock_redshift_client.execute_statement.call_args[1]['Sql']
        assert "COPY test_schema.test_table" in sql
        assert "FROM 's3://test-bucket/data/file.csv'" in sql
        assert "FORMAT AS CSV" in sql
        assert "DELIMITER ','" in sql
        assert "IGNOREHEADER 1" in sql
    
    @patch('redshift_utils.boto3.Session')
    @patch('redshift_utils.s.get_session')
    def test_copy_from_s3_parquet(self, mock_get_session, mock_boto_session):
        """Test copying Parquet file from S3"""
        # Mock setup
        mock_redshift_client = Mock()
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_redshift_client
        mock_boto_session.return_value = mock_session_instance
        
        mock_redshift_client.execute_statement.return_value = {"Id": "copy-id"}
        mock_redshift_client.describe_statement.return_value = {
            "Status": "FINISHED",
            "Duration": 1500000
        }
        
        # Execute with parquet format
        copy_s3_to_redshift(
            s3_uri="s3://test-bucket/data/file.parquet",
            table_name="test_table",
            schema="test_schema",
            db="db",
            cluster_id="cluster",
            db_user="user",
            ds_role="role",
            file_format="parquet",
            verbose=0
        )
        
        # Verify COPY command for parquet
        sql = mock_redshift_client.execute_statement.call_args[1]['Sql']
        assert "FORMAT AS PARQUET" in sql
        assert "DELIMITER" not in sql  # No delimiter for parquet
        assert "IGNOREHEADER" not in sql  # No header option for parquet


if __name__ == "__main__":
    pytest.main([__file__, "-v"])