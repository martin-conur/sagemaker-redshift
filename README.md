# sagemaker-redshift

A Python utility library for Redshift data operations within SageMaker, enabling seamless UNLOAD and COPY operations between Redshift and S3.

## Features

- **UNLOAD data from Redshift to S3** - Export query results to various file formats from within SageMaker
- **COPY data to Redshift from DataFrame** - Fast data loading using S3 as intermediary
- **COPY data from S3 to Redshift** - Direct loading of existing S3 files
- **SageMaker optimized** - Designed for use within SageMaker notebooks and processing jobs
- Supports multiple file formats: CSV, JSON, Parquet
- Built-in retry logic and error handling
- Progress tracking with configurable verbosity

## Installation

```bash
pip install sagemaker-redshift
```

## Requirements

- boto3
- botocore
- polars
- sagemaker

## Usage

### Important: SageMaker Integration

This library is designed to work seamlessly within Amazon SageMaker environments. All functions require explicit credential parameters for security. Previously hardcoded credentials have been removed.

Required parameters:
- `db`: Redshift database name
- `cluster_id`: Redshift cluster identifier
- `db_user`: Database username
- `role`: IAM role ARN with appropriate permissions

### 1. UNLOAD data from Redshift to S3

```python
from redshift_utils import unload_redshift

# Basic usage within SageMaker
unload_redshift(
    query="SELECT * FROM sales.transactions WHERE date >= '2024-01-01'",
    destination="s3://my-bucket/exports/sales/",
    db="prod",
    cluster_id="my-redshift-cluster",
    db_user="myuser",
    role="arn:aws:iam::123456789012:role/RedshiftS3Role",
    file_format="parquet",
    partition_by="date",
    gzip=True
)

# CSV export with custom delimiter
unload_redshift(
    query="SELECT customer_id, total FROM sales.summary",
    destination="s3://my-bucket/exports/summary.csv",
    db="analytics",
    cluster_id="analytics-cluster",
    db_user="analyst",
    role="arn:aws:iam::123456789012:role/RedshiftS3Role",
    file_format="csv",
    delimiter="|",
    header=True
)
```

### 2. COPY DataFrame to Redshift

```python
import polars as pl
from redshift_utils import copy_to_redshift

# Create a sample DataFrame in SageMaker
df = pl.DataFrame({
    "product_id": [1, 2, 3, 4, 5],
    "product_name": ["Widget A", "Widget B", "Gadget C", "Gadget D", "Tool E"],
    "price": [19.99, 29.99, 39.99, 49.99, 59.99],
    "quantity": [100, 150, 75, 200, 50]
})

# Copy to Redshift from SageMaker
copy_to_redshift(
    df=df,
    table_name="products",
    schema="inventory",
    s3_bucket="my-temp-bucket",
    db="warehouse",
    cluster_id="warehouse-cluster",
    db_user="etl_user",
    role="arn:aws:iam::123456789012:role/RedshiftS3Role",
    if_exists="truncate"  # Options: "append", "truncate", "replace"
)
```

### 3. COPY from S3 to Redshift

```python
from redshift_utils import copy_s3_to_redshift

# Copy existing S3 file to Redshift from SageMaker
copy_s3_to_redshift(
    s3_uri="s3://my-data-bucket/raw/customers_2024.parquet",
    table_name="customers",
    schema="staging",
    db="analytics",
    cluster_id="analytics-cluster",
    db_user="loader",
    role="arn:aws:iam::123456789012:role/RedshiftS3Role",
    file_format="parquet",
    if_exists="append"
)
```

## Function Parameters

### Common Parameters

- `db` (str): Redshift database name
- `cluster_id` (str): Redshift cluster identifier
- `db_user` (str): Database username
- `role` (str): IAM role ARN with appropriate permissions (can use SageMaker execution role)
- `verbose` (int): Output verbosity (0=silent, 1=minimal, 2=detailed)
- `max_wait_minutes` (int): Maximum time to wait for operation completion

### unload_redshift

- `query` (str): SQL query to execute
- `destination` (str): S3 URI for output files
- `file_format` (str): Output format - "csv", "json", or "parquet"
- `header` (bool): Include column headers (CSV only)
- `delimiter` (str): Field delimiter (CSV only)
- `allow_overwrite` (bool): Overwrite existing S3 files
- `parallel` (bool): Enable parallel unload
- `partition_by` (str): Column name for partitioning output
- `gzip` (bool): Compress output files

### copy_to_redshift

- `df` (pl.DataFrame): Polars DataFrame to upload
- `table_name` (str): Target table name
- `schema` (str): Target schema name
- `s3_bucket` (str): S3 bucket for temporary storage
- `s3_prefix` (str): S3 key prefix for temporary files
- `if_exists` (str): Action if table exists - "append", "truncate", or "replace"
- `cleanup_s3` (bool): Delete temporary S3 file after load

### copy_s3_to_redshift

- `s3_uri` (str): Full S3 URI of source file
- `table_name` (str): Target table name
- `schema` (str): Target schema name
- `file_format` (str): Source file format - "csv", "json", or "parquet"
- `if_exists` (str): Action if table exists - "append", "truncate", or "replace"

## SageMaker Integration

### Using SageMaker Execution Role

```python
import sagemaker

# Get the SageMaker execution role
role = sagemaker.get_execution_role()

# Use it in your operations
unload_redshift(
    query="SELECT * FROM my_table",
    destination="s3://my-bucket/data/",
    role=role,  # SageMaker execution role
    # ... other parameters
)
```

### Within SageMaker Processing Jobs

This library works seamlessly within SageMaker Processing jobs for large-scale data operations.

## IAM Role Requirements

The IAM role specified in `role` must have:
- Read/write access to the S3 buckets used
- Permission to assume the role from Redshift
- Appropriate Redshift permissions

Example IAM policy:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-bucket/*",
                "arn:aws:s3:::my-bucket"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "redshift-data:*"
            ],
            "Resource": "*"
        }
    ]
}
```

## Error Handling

All functions include comprehensive error handling:
- Validation of required parameters
- Timeout handling with configurable wait times
- Detailed error messages for troubleshooting
- Automatic retry logic for transient failures

## Best Practices

1. **Use appropriate file formats**: Parquet for large datasets, CSV for compatibility
2. **Enable compression**: Use `gzip=True` for UNLOAD to reduce S3 storage costs
3. **Partition large exports**: Use `partition_by` to split large datasets
4. **Clean up temporary files**: Keep `cleanup_s3=True` for copy operations
5. **Set reasonable timeouts**: Adjust `max_wait_minutes` based on data volume
6. **Use SageMaker execution role**: Leverage `sagemaker.get_execution_role()` for permissions

## Testing

Run the test suite:
```bash
pytest test_redshift_utils.py -v
```

## License

MIT