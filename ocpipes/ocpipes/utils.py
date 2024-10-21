from __future__ import annotations

import datetime
import itertools
import pickle
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import awswrangler as wr
import boto3
import botocore
import pandas as pd
from dateutil.tz import tzutc
from metaflow import Flow, Run, get_namespace, namespace

from ocpipes import logger


def parse_date(date_input: Any, format_str: str | None = None) -> pd.Timestamp:
    """Quality of life date parser for get_data_by_interval.

    Args:
        date_input (date-like): The date as a readable format for pd.to_datetime
        (integer, float, string, datetime, list, tuple, 1-d array, Series or DataFrame/dict-like)
        format_str (str): strftime to parse time, eg "%d/%m/%Y"
    Returns:
        pd.Timestamp: input as Timestamp object or array of Timestamps
    """
    parsed_date = pd.to_datetime(date_input, format=format_str)
    if isinstance(parsed_date, datetime.datetime):
        pass
    elif isinstance(parsed_date, pd.DatetimeIndex):
        parsed_date = parsed_date.to_pydatetime()
    else:
        parsed_date = parsed_date.dt.to_pydatetime()

    logger.info("Parsing %s as %s", date_input, parsed_date)
    return parsed_date


def date_ranges_between(
    start: str | (datetime.datetime | datetime.date),
    end: str | (datetime.datetime | datetime.date),
    delta: int | datetime.timedelta = datetime.timedelta(days=7),
    verbose: bool = True,
) -> Iterator[tuple[pd.Timestamp, pd.Timestamp]]:
    """Generator that yields tuples of start/end periods between a
    range of dates with period length specified by a timedelta.

    Args:
        start (date): The date to start the first period from.
        end (date): The date to end the last period at.
        delta (timedelta or int): The length of each time period to generate.
        verbose (bool): Logs the iterator progress.

    Yields:
        (pd.Timestamp, pd.Timestamp): Tuple containing the next period of dates.
    """
    if isinstance(delta, int):
        delta = datetime.timedelta(days=delta)

    parsed_start = parse_date(start)
    parsed_end = parse_date(end)

    current = parsed_start
    while current < parsed_end:
        if current + delta > parsed_end:
            delta = parsed_end - current
        if verbose:
            logger.info(f"{current} to {current + delta}.")
        yield current, current + delta
        current += delta


def batch_idx(
    sequence: Sequence[Any], batch_size: int = 1, verbose: bool = True
) -> Iterator[tuple[int, int]]:
    """Generator that yields tuples of start/end indexes to separate the sequence into batches.

    Args:
        sequence (sequence): A sequence to be batched -- required that it has a `length`.
        batch_size (int): Length of each of the resulting batches.
        verbose (bool): Logs the iterator progress.

    Yields:
        (int, int): Tuple containing the start and end index of the next batch.
    """
    seq_len = len(sequence)
    for idx in range(0, seq_len, batch_size):
        if verbose:
            logger.info(
                f"Batching {idx}-{min(idx + batch_size, seq_len)} of {seq_len}."
            )
        yield idx, min(idx + batch_size, seq_len)


def batch(
    sequence: Sequence[Any], batch_size: int = 1, verbose: bool = True
) -> Iterator[Sequence[Any]]:
    """Breaks a sequence up into separate sequences each of length batch_size.

    Args:
        sequence (sequence): A sequence to be batched -- required that it has a `length`.
        batch_size (int): Length of each of the resulting batches.
        verbose (bool): Logs the iterator progress.

    Yields:
        An object of the same type as sequence, with length `batch_size` (or shorter if it's the terminal batch).
    """
    for batch_idx_start, batch_idx_end in batch_idx(
        sequence=sequence, batch_size=batch_size, verbose=verbose
    ):
        yield sequence[batch_idx_start:batch_idx_end]


def load_parquet(path: str, **kwargs) -> pd.DataFrame:
    """Returns dataframe of the parquet file(s) using awswrangler.

    Essentially deprecating this function because we previously used it so often in the codebase.
    From now on we should use awswrangler directly.

    Args:
        path (str): The absolute path of the parquet file, including the filename
            and extension.

    Returns:
        pandas.DataFrame: Loaded from the parquet file.
    """
    print("This function will be deprecated. Please use awswrangler directly.")
    print(">> import awswrangler as wr")
    print(">> df = wr.s3.read_parquet(path, **kwargs)")
    return wr.s3.read_parquet(path, **kwargs)


def write_parquet(df: pd.DataFrame, path: str, **kwargs):
    """Persists the dataframe using the parquet file format using awswrangler.

    Args:
        df (pandas.DataFrame): Dataframe to store as parquet.
        path (str): The absolute path for the destination of the parquet file,
            including the filename and extension.
    """
    print("This function will be deprecated. Please use awswrangler directly.")
    print(">> import awswrangler as wr")
    print(">> wr.s3.to_parquet(df, path, **kwargs)")
    wr.s3.to_parquet(df, path=path, **kwargs)


def now():
    """A simple function to make a print-friendly, fixed-length string of the time."""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def pairwise(iterable):
    """Returns list of tuples containing pairs of elements from some iterable object.
    E.g. s -> (s0,s1), (s1,s2), (s2, s3), ...
    """
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def get_age_days(bucket, prefix):
    last_modified_ts = (
        boto3.Session()
        .client("s3")
        .get_object(Bucket=bucket, Key=prefix)["LastModified"]
    )
    now_ts = datetime.datetime.now(tzutc())
    return (now_ts - last_modified_ts).days


def check_s3_path(bucket, key):
    file_exists = False
    location = f"s3://{bucket}/{key}"

    s3 = boto3.Session().client("s3")
    try:
        s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except botocore.exceptions.ClientError:
        logger.info(f"{location} does not exist.")
    else:
        file_exists = True
        logger.info(f"{location} exists")

    return file_exists


def list_s3_folders(bucket, prefix):
    if len(prefix) > 0 and prefix[-1] != "/":
        logger.warning(
            'Should include ending "/" in prefix if wanting all folders in prefix'
        )
    s3 = boto3.Session().client("s3")
    response = s3.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")
    # Get the last folder name in each path
    return [
        Path(folder.get("Prefix")).parts[-1]
        for folder in response.get("CommonPrefixes", [])
    ]


def list_s3_objects(bucket, prefix):
    if len(prefix) > 0 and prefix[-1] != "/":
        logger.warning(
            'Should include ending "/" in prefix if wanting all files in prefix'
        )
    s3 = boto3.Session().client("s3")
    response = s3.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")
    # Get the last folder name in each path
    return [obj["Key"] for obj in response["Contents"]]


def bytes_from_s3(bucket, key):
    s3 = boto3.Session().client("s3")
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def load_pickle_s3(bucket, key):
    file_string = bytes_from_s3(bucket, key)
    return pickle.loads(file_string)


def dump_pickle_s3(pickle_object, bucket, key):
    checkpoint_byte_pickle = pickle.dumps(pickle_object)
    bytes_to_s3(checkpoint_byte_pickle, bucket, key)


def bytes_to_s3(bytes_object, bucket, key):
    s3_resource = boto3.resource("s3")
    (s3_resource.Object(bucket_name=bucket, key=key).put(Body=bytes_object))


@contextmanager
def commit_lock(version: int):
    lock = threading.Lock()
    lock.acquire(blocking=True, timeout=60)
    try:
        yield
    except Exception:
        pass
    finally:
        lock.release()


class TemporaryNamespace:
    def __init__(self, temp_namespace: str | None) -> None:
        self.current_namespace = get_namespace()
        # namespace(None) will give you the global namespace
        self.new_namespace = temp_namespace

    def __enter__(self):
        namespace(self.new_namespace)

    def __exit__(self, type, value, traceback):
        namespace(self.current_namespace)


def load_metaflow_object(
    flow: str,
    origin_run_id: int | None = None,
    data_object: str | None = None,
    flow_namespace=None,
) -> Any:
    """Logic for fetching data from a completed metaflow run
    Example uses:
    - load_metaflow_object('MyFlow', data_object='results'): Fetch Run.data.results from the last
    successful MyFlow run
    - load_metaflow_object('MyFlow', origin_run_id=100, data_object='results'): Fetch Run.data.
    results from Run('MyFlow/100')
    - load_metaflow_object('MyFlow', data_object='results', flow_namespace='user:NotMe'): Fetch Run.data.results from
        the last successful MyFlow run in NotMe's namespace
    Args:
        flow (str): Flow class name
        origin_run_id (int, optional): Run id to use from the `flow` if not using the last success. Defaults to None.
        data_object (str, optional): Name of the object to load -- should exist in Run.data. Defaults to None.
        flow_namespace (str, optional): Namespace to pull from, if not the current. Formatted like 'user:{name}' or
            'production:{flow}-{id}-{token}'
    Returns:
        Any: The specified data attribute from the flow.
    """
    with TemporaryNamespace(flow_namespace):
        if not origin_run_id:
            data_run_id = Flow(flow).latest_successful_run.id
            print(f"Inferred {flow} run id: {data_run_id}")
        else:
            data_run_id = origin_run_id
            assert Run(
                f"{flow}/{data_run_id}"
            ).successful, f"{flow} flow was found to be unsuccessful"

        print(f"Loading data from {flow} flow: " f"{flow}/{data_run_id}")
        run = Run(f"{flow}/{data_run_id}")
        return getattr(run.data, data_object)


def list_batch_jobs(
    job_queue: str,
    job_status: str = "RUNNING",
    job_name_contains: str | None = None,
    sort: str = "stoppedAt",
) -> pd.DataFrame:
    """Fetch batch jobs with a specified status and optional name filter.

    Args:
        job_queue (str): Job queue to query
        job_status (str, optional): A (required) filter for jobs to list. Defaults to "RUNNING".
        job_name_contains (str, optional): Optional filter on the job name. Uses `contains` so partial matches work
            as well. Defaults to None.
        sort (str, optional): [description]. Defaults to "stoppedAt".

    Returns:
        pd.DataFrame: DataFrame with the columns ['jobId', 'jobName', 'createdAt', 'status', 'statusReason',
        'startedAt',
       'stoppedAt', 'container']
            `container` will have error information, if job_status is FAILED
    """
    client = boto3.Session().client("batch")
    res = client.list_jobs(jobQueue=job_queue, jobStatus=job_status)
    res_df = pd.DataFrame.from_records(res["jobSummaryList"])
    res_df["createdAt"] = pd.to_datetime(res_df["createdAt"], unit="ms")
    res_df["startedAt"] = pd.to_datetime(res_df["startedAt"], unit="ms")
    res_df["stoppedAt"] = pd.to_datetime(res_df["stoppedAt"], unit="ms")

    if job_name_contains:
        res_df = res_df[res_df["jobName"].str.contains(job_name_contains)]
    return res_df.sort_values(sort)
