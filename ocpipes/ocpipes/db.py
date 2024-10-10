import datetime
from typing import Any, Dict, Iterator, List, Optional, Sequence, Union

import pandas as pd
import psycopg2
from joblib import Parallel, delayed
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import NullPool
from tenacity import retry, wait_exponential
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt

from ocpipes.secrets import DBConnectionSecrets
from ocpipes.utils import batch, date_ranges_between

AGENTS_BY_NOW_COLS = ["agent_id"]
AGENTS_BY_NOW_QUERY = """
select id as agent_id from agent where created_at < %(today)s
"""


LEADS_BY_NOW_COLS = ["lead_id"]
LEADS_BY_NOW_QUERY = """
select id as lead_id from lead where created_at < %(today)s
"""


def date_range_dict_wrapper(
    start: str, end: str, kwargs: Dict[str, Any], delta: Union[int, datetime.timedelta] = datetime.timedelta(days=7)
) -> Iterator[Dict[str, Any]]:
    """
    Wraps the date range generator with arbitrary dictionary passthrough.

    Args:
        start (date): The date to start the first period from.
        end (date): The date to end the last period at.
        delta (timedelta or int): The length of each time period to generate.
        kwargs (dict): The dictionary of params to insert start/end dates into.
    Yields:
        dict: passthrough with the next period of dates inserted.
    """
    for start, end in date_ranges_between(start, end, delta):
        kwargs["start_date"] = start
        kwargs["end_date"] = end
        yield kwargs


def get_data_by_interval(
    query: str,
    columns: Optional[List[str]] = None,
    days_between: Union[int, datetime.timedelta] = datetime.timedelta(days=7),
    parallelization_factor: int = 1,
    add_date_interval: bool = True,
    chunksize: Optional[int] = None,
    **kwargs: Dict[str, Any],
) -> pd.DataFrame:
    """
    Executes queries in parallel for each time interval and returns the concatenated dataframe with all results.

    Args:
        query (str): The query to execute with parameters substituted from kwargs.
        columns (list, optional): The list of column names for the query results.
        days_between (timedelta or int): Length of each time period.
        parallelization_factor (int): Positive numbers indicate a specific number CPU cores to use and
            negative numbers are relative to the current machine. For example, 1 is equivalent to normal,
            -1 uses all cores, and -2 uses all but 1.
        add_date_interval (bool): True to add the start and end dates to the resulting dataframe as columns
        chunksize (int, optional): The (optional) number of rows to include in each subquery.
        kwargs (dict): dictionary used for string substitution of query parameters.Returns:
    Returns:
        pandas.DataFrame: containing the concatenated query results.
    """
    start = kwargs["start_date"]
    end = kwargs["end_date"]
    results = Parallel(n_jobs=parallelization_factor, pre_dispatch="2*n_jobs", batch_size="auto")(
        delayed(get_data)(
            query=query, columns=columns, add_date_interval=add_date_interval, chunksize=chunksize, **date_range_params
        )
        for date_range_params in date_range_dict_wrapper(start, end, kwargs, days_between)
    )

    # Concatenate the results and add the start/end dates as columns.
    results = pd.concat(results)
    return results


def agent_batch_dict_wrapper(
    agent_ids: Sequence[int], batch_size: int, kwargs: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Wraps the batcher with arbitrary dictionary passthrough.

    Args:
        agent_ids (sequence): Subset of agent ids to use, if None (default in `get_data_by_agents`), will use all
            agent ids as of the kwargs['end_date'] (or today, if not provided).
        batch_size (int): size of agent id batches to use.
        kwargs (dict): The dictionary of params to insert agent ids into.
    Yields:
        dict: passthrough with the next batch of agents inserted.
    """

    if agent_ids is None:
        agent_ids = get_data(
            AGENTS_BY_NOW_QUERY, AGENTS_BY_NOW_COLS, **{"today": kwargs.get("end_date", datetime.datetime.today())}
        )
        agent_ids = tuple(agent_ids["agent_id"].unique().tolist())

    for agent_batch in batch(agent_ids, batch_size):
        kwargs["agent_ids_tuple"] = agent_batch
        yield kwargs


def get_data_by_agents(
    query: str,
    columns: Optional[List[str]] = None,
    agent_ids: Optional[Sequence[int]] = None,
    batch_size: int = 1000,
    parallelization_factor: int = 1,
    add_date_interval: bool = True,
    chunksize: Optional[int] = None,
    **kwargs: Dict[str, Any],
) -> pd.DataFrame:
    """
    Executes queries in parallel for each batch of agent ids and returns the concatenated dataframe with all results.

    Args:
        query (str): The query to execute with parameters substituted from kwargs, one of which should be
            %(agent_ids_tuple)s).
        columns (list, optional): The list of column names for the query results.
        agent_ids (sequence): Subset of agent ids to use, if None (default), will use all agent ids up until
            the kwargs['end_date'].
        batch_size (int): size of agent id batches to use.
        parallelization_factor (int): Positive numbers indicate a specific number CPU cores to use and
            negative numbers are relative to the current machine. For example, 1 is equivalent to normal,
            -1 uses all cores, and -2 uses all but 1.
        add_date_interval (bool): True to add the start and end dates to the resulting dataframe as columns
        chunksize (int, optional): The (optional) number of rows to include in each subquery.
        kwargs (dict): dictionary used for string substitution of query parameters.
    Returns:
        pandas.DataFrame: containing the concatenated query results.
    """

    results = Parallel(n_jobs=parallelization_factor, pre_dispatch="2*n_jobs", batch_size="auto")(
        delayed(get_data)(
            query=query, columns=columns, add_date_interval=add_date_interval, chunksize=chunksize, **agent_batch_params
        )
        for agent_batch_params in agent_batch_dict_wrapper(agent_ids, batch_size, kwargs)
    )

    # Concatenate the results
    results = pd.concat(results)
    return results


def lead_batch_dict_wrapper(
    lead_ids: Sequence[int], batch_size: int, kwargs: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Wraps the batcher with arbitrary dictionary passthrough.

    Args:
        lead_ids (sequence): Subset of lead ids to use, if None (default in `get_data_by_leads`), will use all
            lead ids as of the kwargs['end_date'] (or today, if not provided).
        batch_size (int): size of agent id batches to use.
        kwargs (dict): The dictionary of params to insert lead ids into.
    Yields:
        dict: passthrough with the next batch of agents inserted.
    """

    if lead_ids is None:
        lead_ids = get_data(
            LEADS_BY_NOW_QUERY, LEADS_BY_NOW_COLS, **{"today": kwargs.get("end_date", datetime.datetime.today())}
        )
        lead_ids = tuple(lead_ids["lead_id"].unique().tolist())

    for lead_batch in batch(lead_ids, batch_size):
        kwargs["lead_ids_tuple"] = lead_batch
        yield kwargs


def get_data_by_leads(
    query: str,
    columns: Optional[List[str]] = None,
    lead_ids: Optional[Sequence[int]] = None,
    batch_size: int = 1000,
    parallelization_factor: int = 1,
    add_date_interval: bool = True,
    chunksize: Optional[int] = None,
    **kwargs: Dict[str, Any],
) -> pd.DataFrame:
    """
    Executes queries in parallel for each batch of agent ids and returns the concatenated dataframe with all results.

    Args:
        query (str): The query to execute with parameters substituted from kwargs, one of which should be
            %(lead_ids_tuple)s).
        columns (list, optional): The list of column names for the query results.
        lead_ids (sequence): Subset of lead ids to use, if None (default), will use all lead ids up until
            the kwargs['end_date'].
        batch_size (int): size of lead id batches to use.
        parallelization_factor (int): Positive numbers indicate a specific number CPU cores to use and
            negative numbers are relative to the current machine. For example, 1 is equivalent to normal,
            -1 uses all cores, and -2 uses all but 1.
        add_date_interval (bool): True to add the start and end dates to the resulting dataframe as columns
        chunksize (int, optional): The (optional) number of rows to include in each subquery.
        kwargs (dict): dictionary used for string substitution of query parameters.
    Returns:
        pandas.DataFrame: containing the concatenated query results.
    """

    results = Parallel(n_jobs=parallelization_factor, pre_dispatch="2*n_jobs", batch_size="auto")(
        delayed(get_data)(
            query=query, columns=columns, add_date_interval=add_date_interval, chunksize=chunksize, **lead_batch_params
        )
        for lead_batch_params in lead_batch_dict_wrapper(lead_ids, batch_size, kwargs)
    )

    # Concatenate the results
    results = pd.concat(results)
    return results


def inquiry_guid_batch_dict_wrapper(
    rdc_inquiry_guids: Sequence[str], batch_size: int, kwargs: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Wraps the batcher with arbitrary dictionary passthrough.

    Args:
        rdc_inquiry_guids (sequence[str]): Subset of inquiry guids to use.
        batch_size (int): size of inquiry guid batches to use.
        kwargs (Dict[str, any]): The dictionary of params to insert inquiry guids into.
    Yields:
        dict: passthrough with the next batch of inquiry guids inserted.
    """
    for inquiry_guid_batch in batch(rdc_inquiry_guids, batch_size):
        kwargs["rdc_inquiry_guid_tuple"] = inquiry_guid_batch
        yield kwargs


def get_data_by_inquiry_guid(
    query: str,
    columns: Optional[List[str]] = None,
    rdc_inquiry_guids: Optional[Sequence[int]] = None,
    batch_size: int = 1000,
    parallelization_factor: int = 1,
    add_date_interval: bool = True,
    chunksize: Optional[int] = None,
    **kwargs: Dict[str, Any],
) -> pd.DataFrame:
    """
    Executes queries in parallel for each batch of rdc_inquiry_guid and returns the concatenated dataframe with all results.

    Args:
        query (str): The query to execute with parameters substituted from kwargs, one of which should be
            %(rdc_inquiry_guid_tuple)s).
        columns (list, optional): The list of column names for the query results.
        rdc_inquiry_guids (sequence): Subset of inquiry guids to use.
        batch_size (int): size of inquiry guid batches to use.
        parallelization_factor (int): Positive numbers indicate a specific number CPU cores to use and
            negative numbers are relative to the current machine. For example, 1 is equivalent to normal,
            -1 uses all cores, and -2 uses all but 1.
        add_date_interval (bool): True to add the start and end dates to the resulting dataframe as columns
        chunksize (int, optional): The (optional) number of rows to include in each subquery.
        kwargs (dict): dictionary used for string substitution of query parameters.
    Returns:
        pandas.DataFrame: containing the concatenated query results.
    """

    results = Parallel(n_jobs=parallelization_factor, pre_dispatch="2*n_jobs", batch_size="auto")(
        delayed(get_data)(
            query=query,
            columns=columns,
            add_date_interval=add_date_interval,
            chunksize=chunksize,
            **inquiry_guid_batch_params,
        )
        for inquiry_guid_batch_params in inquiry_guid_batch_dict_wrapper(rdc_inquiry_guids, batch_size, kwargs)
    )

    # Concatenate the results
    results = pd.concat(results)
    return results


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=2),
    retry=retry_if_exception_type(psycopg2.OperationalError),
)
def get_data(
    query: str,
    columns: Optional[List[str]] = None,
    add_date_interval: bool = True,
    chunksize: Optional[int] = None,
    **kwargs: Dict[str, Any],
):
    """
    Quality of life retry logic for the query if an operational error occurs.
    Includes connectivity issues (i.e. broken pipe from SSH tunnel) and
    errors related to replica DB synchronization.

    Args:
        query (str): The query to execute with parameters substituted from kwargs.
        columns (list, optional): The (optional) list of column names for the query results.
        add_date_interval (bool): True to add the start and end dates to the resulting dataframe as columns
        chunksize (int, optional): The (optional) number of rows to include in each subquery.
        kwargs (dict): dictionary used for string substitution of query parameters.
    Returns:
        pandas.DataFrame: containing the results of the query.
    """
    return_df = database_results(query, columns, chunksize, **kwargs)
    if "start_date" in kwargs and "end_date" in kwargs and add_date_interval:
        return_df["from_day"] = kwargs["start_date"]
        return_df["to_day"] = kwargs["end_date"]
    return return_df


def get_engine():
    connection_info = DBConnectionSecrets()
    connection = URL(
        drivername=connection_info.engine,
        username=connection_info.username,
        password=connection_info.password,
        host=connection_info.host,
        port=connection_info.port,
        database=connection_info.dbname,
    )

    # https://github.com/pandas-dev/pandas/issues/12265
    # need server side / named cursors
    # Use NullPool to replace disposing engine on exit - equivalent to finally: engine.dispose() for a one-off
    return create_engine(connection, poolclass=NullPool, server_side_cursors=True)


def database_results(
    query: str, result_columns: Optional[List[str]] = None, chunksize: Optional[int] = None, **kwargs: Dict[str, Any]
) -> pd.DataFrame:
    """
    Simple wrapper to query the database and to dump the results in a dataframe.

    Args:
        query (str): The query to execute with parameters substituted from kwargs.
        result_columns (list, optional): The (optional) list of column names for the query results.
        chunksize (int, optional): The (optional) number of rows to include in each subquery.
        kwargs (dict): dictionary used for string substitution of query parameters.
    Returns:
        pandas.DataFrame: Query results in pandas form.
    """
    engine = get_engine()

    res = pd.read_sql_query(query, engine, params=kwargs, chunksize=chunksize)

    if chunksize is not None:
        df = pd.concat(res, ignore_index=True)
    else:
        df = res

    if result_columns:
        df.columns = result_columns
    return df
