import os
import uuid
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import awswrangler as wr
import boto3
import pandas as pd
from botocore.credentials import DeferredRefreshableCredentials
from botocore.session import get_session
from joblib import Parallel, delayed

from ocpipes import logger
from ocpipes.utils import batch

RDC_DATAENG_IAM_ROLE = "arn:aws:iam::057425096214:role/move-dataeng-assumerole-opcity-ds"
RDC_DATAENG_DEFAULT_REGION = "us-west-2"
RDC_DATAENG_ROLE_SESSION_NAME_PREFIX = "opcity-ds"
RDC_DATAENG_ROLE_SESSION_DURATION = 3600  # 1hr is the max allowed for role chaining.
RDC_DATAENG_DEFAULT_S3_PATH = "s3://move-dataeng-opcity-dap-prod/datascience"
RDC_DATAENG_DEFAULT_DATABASE = "opcity_ds"  # AWS Glue/Athena database name


RDC_ENTSYS_IAM_ROLE = "arn:aws:iam::608385023053:role/es-datahub-op-ds-prod"
RDC_ENTSYS_DEFAULT_REGION = "us-west-2"
RDC_ENTSYS_ROLE_SESSION_NAME_PREFIX = "opcity-ds"
RDC_ENTSYS_ROLE_SESSION_DURATION = 3600  # 1hr is the max allowed for role chaining.
RDC_ENTSYS_DEFAULT_S3_PATH = "s3://es-datahub-internal-prod/opcity"
RDC_ENTSYS_DEFAULT_DATABASE = "ds_opcity"  # AWS Glue/Athena database name


def get_rdc_session(
    duration_seconds: Optional[int] = None, region_name: Optional[str] = None, aws_account: Optional[str] = "DATAENG"
) -> boto3.Session:
    """
    Creates a boto session for the RDC DataEng account with automatic credential refreshing.

    Args:
        duration_seconds (int, optional): TTL of session temporary security credentials.
        region_name (str, optional): Default region when creating new connections from the session.
        aws_account (str, optional): One of "DATAENG", "ENTSYS" -- specifying which AWS role to asssume. Default is for
            DATAENG prod.
    Returns:
        boto3.Session: with our team-specific IAM role for the specified RDC account.
    """
    if aws_account == "DATAENG":
        default_region_name = RDC_DATAENG_DEFAULT_REGION
        default_duration_seconds = RDC_DATAENG_ROLE_SESSION_DURATION
        role_name_prefix = RDC_DATAENG_ROLE_SESSION_NAME_PREFIX
        iam_role = RDC_DATAENG_IAM_ROLE

    elif aws_account == "ENTSYS":
        default_region_name = RDC_ENTSYS_DEFAULT_REGION
        default_duration_seconds = RDC_ENTSYS_ROLE_SESSION_DURATION
        role_name_prefix = RDC_ENTSYS_ROLE_SESSION_NAME_PREFIX
        iam_role = RDC_ENTSYS_IAM_ROLE

    else:
        raise ValueError("`aws_account` must be one of 'DATAENG', 'ENTSYS', instead got '{aws_account}'")

    if duration_seconds is None:
        duration_seconds = default_duration_seconds

    if region_name is None:
        region_name = default_region_name

    user = os.getenv("USER", "opcity")
    session_name = f"{role_name_prefix}-{user}-{uuid.uuid1().hex[:8]}"
    logger.debug(f"Creating cross-account session with name: {session_name}")

    def refresh():
        sts = boto3.client("sts")
        sts_response = sts.assume_role(RoleArn=iam_role, RoleSessionName=session_name, DurationSeconds=duration_seconds)
        return dict(
            access_key=sts_response["Credentials"]["AccessKeyId"],
            secret_key=sts_response["Credentials"]["SecretAccessKey"],
            token=sts_response["Credentials"]["SessionToken"],
            expiry_time=sts_response["Credentials"]["Expiration"].isoformat(),
        )

    session_credentials = DeferredRefreshableCredentials(refresh_using=refresh, method="sts-assume-role")

    # Doesn't seem to be any workaround that doesn't mess with botocore internals for now.
    # https://github.com/boto/boto3/issues/443
    # https://github.com/boto/botocore/issues/761
    session = get_session()
    session._credentials = session_credentials
    refreshable_session = boto3.Session(botocore_session=session, region_name=region_name)
    return refreshable_session


def get_rdc_data(
    sql: str,
    append_s3_path: Optional[str] = "",
    categories: Optional[List[str]] = None,
    ctas_approach: Optional[bool] = True,
    chunksize: Optional[Union[int, bool]] = None,
    workgroup: Optional[str] = None,
    keep_files: bool = False,
    ctas_temp_table_name: Optional[str] = None,
    use_threads: bool = True,
    params: Optional[Dict[str, Any]] = None,
    session_duration_seconds: Optional[int] = None,
    aws_account: Optional[str] = "DATAENG",
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """
    Queries RDC's Athena using CTAS with compressed parquet to a user-specific s3 location, wrapping the call to AWS
    Data Wrangler's `read_sql_query`.

    After the query is complete, additional execution metadata is accessible using `df.query_metadata`.

    Args:
        sql (str): SQL query.
        append_s3_path (str, optional): Additional S3 path to append after user-specific directory.
        ctas_approach (bool, optional): Wraps the query using a CTAS, and read the resulted parquet data on S3.
            If false, read the regular CSV on S3.
        categories (List[str], optional): List of columns names that should be returned as pandas.Categorical.
            Recommended for memory restricted environments.
        chunksize (int | bool, optional): If passed will split the data in a Iterable of DataFrames (Memory friendly).
            If `True` wrangler will iterate on the data by files in the most efficient way without guarantee of
            chunksize.
            If an `INTEGER` is passed Wrangler will iterate on the data by number of rows equal to the received INTEGER.
        workgroup (str, optional): Athena workgroup.
        keep_files (bool): If Wrangler delete or keep the staging files produced by Athena.
        ctas_temp_table_name (str, optional): The name of the temporary table and also the directory name on S3 where
            the CTAS result is stored. If None, it will use a random pattern: `f"temp_table_{pyarrow.compat.guid()}"`.
            This will be within the user-specific directory.
        use_threads (bool): True to enable concurrent requests, False to disable multiple threads. If enabled
            os.cpu_count() will be used as the max number of threads.
        params (Dict[str, any], optional): Dict of parameters that will be used for constructing the SQL query. Only
            named parameters are supported. The dict needs to contain the information in the form {'name': 'value'} and
            the SQL query needs to contain `:name;`.
        session_duration_seconds (int, optional): TTL of session temporary security credentials, 6hrs is max allowed.
        aws_account (str, optional): One of "DATAENG", "ENTSYS" -- specifying which AWS account to read from. Default is
            DATAENG prod.
    Returns:
        Union[pd.DataFrame, Iterator[pd.DataFrame]]: Pandas DataFrame or Generator of Pandas DataFrames if chunksize is
            passed.
    """
    if aws_account == "DATAENG":
        s3_path = RDC_DATAENG_DEFAULT_S3_PATH
        default_db = RDC_DATAENG_DEFAULT_DATABASE
    elif aws_account == "ENTSYS":
        s3_path = RDC_ENTSYS_DEFAULT_S3_PATH
        default_db = RDC_ENTSYS_DEFAULT_DATABASE
    else:
        raise ValueError("`aws_account` must be one of 'DATAENG', 'ENTSYS', instead got '{aws_account}'")
    if session_duration_seconds:
        session = get_rdc_session(duration_seconds=session_duration_seconds, aws_account=aws_account)
    else:
        session = get_rdc_session(aws_account=aws_account)
    user = os.getenv("USER", "opcity")
    s3_output = f"{s3_path}/{user}/{append_s3_path}"
    logger.info(f"Starting query, intermediate files will be stored to {s3_output}")
    df = wr.athena.read_sql_query(
        sql=sql,
        database=default_db,
        ctas_approach=ctas_approach,
        categories=categories,
        chunksize=chunksize,
        s3_output=s3_output,
        workgroup=workgroup,
        keep_files=keep_files,
        ctas_parameters=wr.typing.AthenaCTASSettings(database=default_db, temp_table_name=ctas_temp_table_name),
        use_threads=use_threads,
        boto3_session=session,
        params=params,
        data_source=None,  # If None, 'AwsDataCatalog' will be used by default.
    )
    if chunksize:
        df = pd.concat(df, ignore_index=True, sort=False, copy=False)
    else:
        logger.info(
            (
                f'Execution completed in {df.query_metadata["Statistics"]["TotalExecutionTimeInMillis"]/1000} seconds '
                f'and scanned {df.query_metadata["Statistics"]["DataScannedInBytes"] / 1024**3 :.3f} GB of data.'
            )
        )
        if ctas_approach:
            logger.debug(f'Intermediate results stored to {df.query_metadata["ResultConfiguration"]["OutputLocation"]}')
            logger.debug(
                "Intermediate results cataloged in data manifest file at "
                f'{df.query_metadata["Statistics"]["DataManifestLocation"]}'
            )
    logger.info("Finished reading query results.")
    return df


def visitor_batch_param_wrapper(
    rdc_visitor_ids: Sequence[str], batch_size: int, params: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Wraps the batcher with arbitrary dictionary passthrough.

    Args:
        rdc_visitor_ids (sequence[str]): Subset of rdc visitor ids to use.
        batch_size (int): size of rdc visitor id batches to use.
        params (Dict[str, any]): The dictionary of params to insert vistor ids into.
    Yields:
        dict: passthrough with the next batch of visitors inserted.
    """
    for visitor_batch in batch(rdc_visitor_ids, batch_size):
        params["rdc_visitor_id_tuple"] = visitor_batch
        yield params


def get_rdc_data_by_visitors(
    sql: str,
    rdc_visitor_ids: Sequence[str],
    batch_size: int = 5000,
    n_jobs: int = -1,
    append_s3_path: Optional[str] = "",
    categories: Optional[List[str]] = None,
    chunksize: Optional[Union[int, bool]] = None,
    params: Optional[Dict[str, Any]] = None,
    session_duration_seconds: Optional[int] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """
    Executes queries in parallel for each batch of rdc visitor ids and returns the concatenated dataframe with all
    results.

    Intermediate files are deleted after reading results and custom ctas table naming is not allowed.

    The max batch size is limited by the max query string length of Athena (262144). Each rdc visitor id is 32 chars
    long with a couple extra for quotes, commas, etc. Depending on the length of the query itself, this puts a cap on
    the batch sizes of ~5k.

    Args:
        sql (str): SQL query to execute with a parameterized `:rdc_visitor_id_tuple;` that will be batch substituted.
        rdc_visitor_ids (sequence[str]): Subset of rdc visitor ids to use.
        batch_size (int): size of visitor id batches to use.
        n_jobs (int): Positive numbers indicate a specific number CPU cores to use and negative numbers are relative to
            the current machine. For example, 1 is equivalent to normal, -1 uses all cores, and -2 uses all but 1.
        append_s3_path (str, optional): Additional S3 path to append after user-specific directory.
        categories (List[str], optional): List of columns names that should be returned as pandas.Categorical.
            Recommended for memory restricted environments.
        chunksize (int | bool, optional): If passed will split the data in a Iterable of DataFrames (Memory friendly).
            If `True` wrangler will iterate on the data by files in the most efficient way without guarantee of
                chunksize.
            If an `INTEGER` is passed Wrangler will iterate on the data by number of rows equal to the received INTEGER.
        params (Dict[str, any], optional): Dict of parameters that will be used for constructing the SQL query. Only
            named parameters are supported. The dict needs to contain the information in the form {'name': 'value'} and
            the SQL query needs to contain `:name;`.
        session_duration_seconds (int, optional): TTL of session temporary security credentials, 6hrs is max allowed.
    Returns:
        Union[pd.DataFrame, Iterator[pd.DataFrame]]: Pandas DataFrame or Generator of Pandas DataFrames if chunksize is
            passed.
    """
    if not params:
        params = {}

    results = Parallel(n_jobs=n_jobs)(
        delayed(get_rdc_data)(
            sql=sql,
            append_s3_path=append_s3_path,
            categories=categories,
            chunksize=chunksize,
            keep_files=False,
            ctas_temp_table_name=None,
            use_threads=(n_jobs != 1),
            params=visitor_batch_params,
            session_duration_seconds=session_duration_seconds,
        )
        for visitor_batch_params in visitor_batch_param_wrapper(rdc_visitor_ids, batch_size, params)
    )
    return pd.concat(results, ignore_index=True, sort=False, copy=False)


def get_visitors_by_date(date: str) -> Tuple[str]:
    """
    The visitors observed on a date, given as a tuple for parameter passthrough in `get_rdc_data_by_visitors`.

    Args:
        date (str): parsed using `pd.to_datetime`.
    Returns:
        Tuple[str]: rdc visitor ids that were observed on the given date.
    """

    sql = """
    SELECT DISTINCT rdc_visitor_id
    FROM biz_data_product_event_v2.rdc_biz_data
    WHERE TRUE
        AND event_date = ':date;'
    """
    params = {"date": pd.to_datetime(date).strftime("%Y%m%d")}
    df = get_rdc_data(sql=sql, params=params)
    return tuple(df["rdc_visitor_id"].tolist())


def get_visitors_by_interval(start_datetime: str, end_datetime: str) -> Tuple[str]:
    """
    The visitors observed within an inclusive date range, given as a tuple for parameter passthrough in
    `get_rdc_data_by_visitors`.

    Args:
        start_datetime (str): parsed using `pd.to_datetime`.
        end_datetime (str): parsed using `pd.to_datetime`.
    Returns:
        Tuple[str]: rdc visitor ids that were observed in the interval.
    """

    sql = """
    SELECT DISTINCT rdc_visitor_id
    FROM biz_data_product_event_v2.rdc_biz_data
    WHERE TRUE
        AND event_date >= ':start_datetime;'
        AND event_date <= ':end_datetime;'
    """
    params = {
        "start_datetime": pd.to_datetime(start_datetime).strftime("%Y%m%d"),
        "end_datetime": pd.to_datetime(end_datetime).strftime("%Y%m%d"),
    }
    df = get_rdc_data(sql=sql, params=params)
    return tuple(df["rdc_visitor_id"].tolist())


def get_query_execution(query_execution_id: str) -> Dict[str, Any]:
    """
    Convenience function to get query metadata from RDC's Athena.

    Args:
        query_execution_id (str): unique ID of a query execution in Athena.
    Returns:
        dict: See docs for included query information.
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_query_execution
    """
    session = get_rdc_session()
    athena = session.client("athena")
    return athena.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]


def to_parquet(
    df: pd.DataFrame,
    filename: str,
    append_s3_path: Optional[str] = "",
    index: bool = False,
    compression: Optional[str] = "snappy",
    max_rows_by_file: Optional[int] = None,
    use_threads: bool = True,
    user: Optional[str] = None,
) -> str:
    """
    Wrapper to write dataframes to our shared S3 bucket in RDC's account using AWS Data Wrangler.

    Args:
        df (pd.DataFrame): Dataframe to write.
        filename (str): Parquet filename (e.g. mydf.parquet)
        append_s3_path (str, optional): Additional S3 path to append after user-specific directory.
        index (bool): True to store the DataFrame index in file, otherwise False to ignore it.
        compression (str, optional): Compression style (None, snappy, gzip).
        use_threads (bool): True to enable concurrent requests, False to disable multiple threads. If enabled
            os.cpu_count() will be used as the max number of threads.
    Returns:
        str: The stored file path on S3.
    """
    session = get_rdc_session()
    if not user:
        user = os.getenv("USER", "opcity")
    if append_s3_path:
        s3_output = f"{RDC_DATAENG_DEFAULT_S3_PATH}/{user}/{append_s3_path}/{filename}"
    else:
        s3_output = f"{RDC_DATAENG_DEFAULT_S3_PATH}/{user}/{filename}"

    logger.info(f"Writing parquet file to {s3_output}")
    result = wr.s3.to_parquet(
        df=df,
        path=s3_output,
        index=index,
        compression=compression,
        max_rows_by_file=max_rows_by_file,
        use_threads=use_threads,
        boto3_session=session,
    )
    output_path = result["paths"][0]
    logger.info(f"Finished writing {output_path}")
    return output_path
