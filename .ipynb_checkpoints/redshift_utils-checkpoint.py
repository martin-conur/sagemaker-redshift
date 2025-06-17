import botocore.session as s
from botocore.exceptions import ClientError
import boto3.session
import boto3
from botocore.exceptions import WaiterError
from botocore.waiter import WaiterModel
from botocore.waiter import create_waiter_with_client
import sagemaker
import time
import re

def unload_redshift(query: str, 
                    destination: str,
                    header: bool=True,
                    file_format: str='csv',
                    delimiter: str=',',
                    allow_overwrite: bool=True,
                    parallel: bool=True,
                    partition_by: str=None,
                    gzip: bool=False,
                    verbose: int=1,
                    max_wait_minutes: int=60)-> None:
   
    """
        Performs redshift UNLOAD given a query and its options.
        Enhanced version with better waiting mechanism for long queries.
        
        Args:
            query: redshift SQL query. Values inside single quotes ('value')
                should be in double single quotes (''value'').
            destination: s3 uri where unload data will be stored
            header: whether store header in the files or not
            file_format: format of the files stored in s3
            delimiter: file delimiter
            allow_overwrite: allow overwrite in the s3 uri will replace files in destination
            parallel: if True, will perform the UNLOAD in a parallel fashion
            partition_by: column to partition by the files
            gzip: whether you want the s3 file(s) compressed or not
            verbose: 0 = no output, 1 = minimal output and 2 = full output
            max_wait_minutes: maximum minutes to wait for completion
        
        Returns:
            None
    """
    
    # Enhanced waiter configuration for long queries
    delay = 30  # Check every 30 seconds
    max_attempts = max_wait_minutes * 2  # Convert minutes to attempts (30s intervals)
    
    waiter_config = {
        'version': 2,
        'waiters': {
            'DataAPIExecution': {
                'operation': 'DescribeStatement',
                'delay': delay,
                'maxAttempts': max_attempts,
                'acceptors': [
                    {
                        "matcher": "path",
                        "expected": "FINISHED",
                        "argument": "Status",
                        "state": "success"
                    },
                    {
                        "matcher": "pathAny",
                        "expected": ["PICKED", "STARTED", "SUBMITTED"],
                        "argument": "Status",
                        "state": "retry"
                    },
                    {
                        "matcher": "pathAny",
                        "expected": ["FAILED", "ABORTED"],
                        "argument": "Status",
                        "state": "failure"
                    }
                ],
            },
        },
    }

    # Setup sessions and clients
    session = boto3.session.Session()
    region = session.region_name
    bc_session = s.get_session()
    
    session = boto3.Session(
        botocore_session=bc_session,
        region_name=region,
    )
    
    client_redshift = session.client("redshift-data")
    s3_client = session.client("s3")
    
    if verbose >= 1:
        print("Data API client successfully loaded")

    # Configuration
    ***REMOVED***
    cluster_id = '***REMOVED***'
    db_user = "***REMOVED***"
    ds_role = "***REMOVED***"

    # Create custom waiter
    waiter_name = 'DataAPIExecution'
    waiter_model = WaiterModel(waiter_config)
    custom_waiter = create_waiter_with_client(waiter_name, waiter_model, client_redshift)
    
    ### Format unload options
    # Header
    if file_format == "parquet":
        header_str = ""
    else:
        header_str = "HEADER" if header else ""
    
    # Format validation
    assert file_format.lower() in ("csv", "json", "parquet"), "file_format not valid."

    # Delimiter
    if file_format.lower() != "parquet":
        delimiter_str = f"DELIMITER '{delimiter}'"
    else:
        delimiter_str = ""
    
    # Allow overwrite
    allow_overwrite_str = "ALLOWOVERWRITE" if allow_overwrite else ""
    
    # Parallel
    parallel_str = "PARALLEL FALSE" if not parallel else ""
    
    # Partition - Fixed the bug here
    if partition_by:
        partition_by_str = f"PARTITION BY '{partition_by}'"
    else:
        partition_by_str = ""
    
    # Gzip
    gzip_str = "GZIP" if gzip else ""
    
    # Extension
    if file_format.lower() == "csv":
        extension_str = "EXTENSION 'csv'"
    elif file_format.lower() == "json":  # Fixed comparison bug
        extension_str = "EXTENSION 'json'"
    else:
        extension_str = ""
    
    # Create the unload query
    query_unload = f"""
        unload ('{query}')
        to '{destination}' iam_role '{ds_role}' 
        format as {file_format} 
        {header_str} 
        {delimiter_str}
        {allow_overwrite_str}
        {parallel_str}
        {partition_by_str}
        {gzip_str}
        {extension_str}
    """
    
    if verbose >= 1:
        print("Starting Redshift UNLOAD...")
        if verbose >= 2:
            print("Query:")
            print(query_unload)
    
    # Execute the unload
    res1 = client_redshift.execute_statement(
        Database=db, 
        DbUser=db_user, 
        Sql=query_unload, 
        ClusterIdentifier=cluster_id
    )
    
    id1 = res1["Id"]
    if verbose >= 1:
        print(f"UNLOAD started with ID: {id1}")
        print(f"Maximum wait time: {max_wait_minutes} minutes")

    # Wait for completion with enhanced error handling
    try:
        if verbose >= 1:
            print("Waiting for UNLOAD to complete...")
        
        custom_waiter.wait(Id=id1)
        
        if verbose >= 1:
            print("Data API execution completed!")
            
    except WaiterError as e:
        print(f"Waiter error occurred: {e}")
        # Get final status even if waiter times out
        desc = client_redshift.describe_statement(Id=id1)
        print(f"Final status: {desc['Status']}")
        if desc['Status'] in ['FAILED', 'ABORTED']:
            if 'Error' in desc:
                print(f"Error: {desc['Error']}")
            raise Exception(f"UNLOAD failed with status: {desc['Status']}")

    # Get final execution details
    desc = client_redshift.describe_statement(Id=id1)
    execution_time = float(desc["Duration"]/pow(10,6)) if "Duration" in desc else 0
    
    print(f"[UNLOAD] Status: {desc['Status']}. Execution time: {execution_time:.0f} milliseconds")
    
    if verbose >= 2:
        print("Full execution details:")
        print(desc)
    
    # Additional verification: Check if files exist in S3
    if desc["Status"] == "FINISHED":
        verify_s3_files(destination, s3_client, verbose)

def verify_s3_files(s3_uri: str, s3_client, verbose: int = 1):
    """
    Verify that files were actually created in S3 destination
    """
    try:
        # Parse S3 URI
        if not s3_uri.startswith('s3://'):
            raise ValueError("Destination must be an S3 URI starting with s3://")
        
        # Remove s3:// and split bucket and prefix
        s3_path = s3_uri[5:]  # Remove 's3://'
        bucket_name = s3_path.split('/')[0]
        prefix = '/'.join(s3_path.split('/')[1:]) if '/' in s3_path else ''
        
        if verbose >= 1:
            print(f"Verifying files in S3: s3://{bucket_name}/{prefix}")
        
        # List objects in the destination
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=10  # Just check if files exist
        )
        
        if 'Contents' in response and len(response['Contents']) > 0:
            file_count = response['Contents'].__len__()
            if verbose >= 1:
                print(f"✅ SUCCESS: Found {file_count} file(s) in destination")
                if verbose >= 2:
                    for obj in response['Contents'][:5]:  # Show first 5 files
                        print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("⚠️  WARNING: No files found in S3 destination")
            
    except Exception as e:
        print(f"⚠️  Could not verify S3 files: {e}")
