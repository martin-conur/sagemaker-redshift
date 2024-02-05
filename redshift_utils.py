import botocore.session as s
from botocore.exceptions import ClientError
import boto3.session
import boto3
from botocore.exceptions import WaiterError
from botocore.waiter import WaiterModel
from botocore.waiter import create_waiter_with_client
import sagemaker


# Create custom waiter for the Redshift Data API to wait for finish execution of current SQL statement
waiter_name = 'DataAPIExecution'
delay=2
max_attempts=10

#Configure the waiter settings
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
          "expected": ["PICKED","STARTED","SUBMITTED"],
          "argument": "Status",
          "state": "retry"
        },
        {
          "matcher": "pathAny",
          "expected": ["FAILED","ABORTED"],
          "argument": "Status",
          "state": "failure"
        }
      ],
    },
  },
}

session = boto3.session.Session()
region = session.region_name

bc_session = s.get_session()

session = boto3.Session(
        botocore_session=bc_session,
        region_name=region,
    )

def unload_redshift(query: str, 
                    destination: str,
                    header: bool=True,
                    file_format: str='csv',
                    delimiter: str=',',
                    allow_overwrite: bool=True,
                    parallel: bool=True,
                    partition_by: str=None,
                    gzip: bool=False,
                    verbose: int=1)-> None:
   
    """
        Performs redshift UNLOAD given a query and its options.
        
        Args:
            query: redshift SQL query. Values inside single quotes ('value')
                should be in double single quotes (''value'').
                
            destination: s3 uri where unload data will be stored:
            
            header: whether store header in the files or not.
            
            file_format: format of the files stored in s3.
            
            delimiter: file delimiter.
            
            allow_overwrite: allow overwrite in the s3 uri will replace files in destination.
            
            parallel: if True, will perform the UNLOAD in a 
                parallel fashion. This will result in multiple files. Set to 
                False if you want 1 file.
                
            partition_by: column to partition by the files.
            
            gzip: whether you want the s3 file(s) compressed or not.
            
            verbose: 0 = no output, 1 = minimal output and 2 = full output.
        
        Returns:
            None
    """
    # Setup the client
    client_redshift = session.client("redshift-data")

    print("Data API client successfully loaded")

    ***REMOVED***
    cluster_id = '***REMOVED***'
    db_user = "***REMOVED***"

    # creating the waiter
    waiter_model = WaiterModel(waiter_config)
    custom_waiter = create_waiter_with_client(waiter_name, waiter_model, client_redshift)
    
    redshift_iam_role = sagemaker.get_execution_role() 
    source_s3_region='us-east-1'
    ds_role = "***REMOVED***"
    
    ### formatting unload options
    # header
    if file_format == "parquet":
        header_str = ""
    else:
        header_str = "HEADER" if header else ""
    # format
    assert file_format.lower() in ("csv", "json", "parquet"), "file_format not valid."

    #delimiter
    if file_format.lower() != "parquet":
        delimiter_str = f"DELIMITER '{delimiter}'"
    else:
        delimiter_str = ""
    # allow overwrite
    if allow_overwrite:
        allow_overwrite_str = "ALLOWOVERWRITE"
    else:
        allow_overwrite_str = ""
    # parallel
    if not parallel:
        parallel_str = "PARALLEL FALSE"
    else:
        parallel_str = ""
    # partition
    if partition_by:
        partition_by_str = "PARTITION BY '{partition_by}''"
    else:
        partition_by_str = ""
    # gzip
    gzip_str = "GZIP" if gzip else ""
    
    # extension
    if file_format.lower() == "csv":
        extension_str = "EXTENSION 'csv'"
    elif file_format.lower == "json":
        extension_str = "EXTENSION 'json'"
    else:
        extension_str = ""
    
    
    ## creating the query
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
        print(query_unload)
    # execution
    res1 = client_redshift.execute_statement(Database=db, DbUser=db_user, Sql=query_unload, ClusterIdentifier=cluster_id)
    if verbose >= 1:
        print("Redshift UNLOAD started ...")

    id1 = res1["Id"]
    if verbose >= 1:
        print("\nID: " + id1)

    # Waiter in try block and wait for DATA API to return
    try:
        custom_waiter.wait(Id=id1, )
        print("Done waiting to finish Data API for the UNLOAD statement.")
    except WaiterError as e:
        print (e)

    desc=client_redshift.describe_statement(Id=id1)
    print("[UNLOAD] Status: " + desc["Status"] + ". Excution time: %d miliseconds" %float(desc["Duration"]/pow(10,6)))
    
    if verbose >= 2:
        print(desc)