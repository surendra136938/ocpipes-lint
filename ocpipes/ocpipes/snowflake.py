"""Utilities for connecting to and using Snowflake.

When converting queries from Postgres to Snowflake, common syntax and structural changes can be found here:
https://moveinc.atlassian.net/wiki/spaces/DataEng/pages/116332560671/Common+Postgres+to+Snowflake+Conversions

For optimizing queries, Snowflake has a profiling tool in the web UI:
https://docs.snowflake.com/en/user-guide/ui-query-profile.html
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import pandas as pd
import pyarrow as pa
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from ocpipes import logger
from ocpipes.secrets import SnowflakeConnectionSecrets


class SnowflakeConnection:
    """Wrapper class for creating and managing Snowflake connections with authentication credentials inferred using
    `ocpipes.secrets.SnowflakeConnectionSecrets` and passthrough methods/attributes to the underlying Snowflake
    connection object.

    Preferably used as a context manager to ensure the connection is closed afterwards, but can also be used directly.

    Examples:
        >>> # context manager ensures the connection is closed.
        >>> with SnowflakeConnection() as con:
        >>>     with con.cursor() as cur:
        >>>         cur.execute(...)

        >>> # try/finally to ensure the connection is closed.
        >>> con = SnowflakeConnection()
        >>> try:
        >>>     con.cursor().execute(...)
        >>> finally:
        >>>     con.close()
    """  # noqa: D205

    def __init__(
        self,
        user: str | None = None,
        role: str | None = None,
        account: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        is_production: bool = False,
    ) -> None:
        connection_info = SnowflakeConnectionSecrets(
            user=user,
            role=role,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            is_production=is_production,
        )
        logger.debug(connection_info)

        self.connection = snowflake.connector.connect(
            user=connection_info.user,
            private_key=connection_info.get_private_key(),
            account=connection_info.account,
            warehouse=connection_info.warehouse,
            database=connection_info.database,
            schema=connection_info.schema,
            role=connection_info.role,
        )

    def __getattr__(self, attr: Any) -> Any:
        """Passing through other methods and attributes to the underlying connection."""
        return getattr(self.connection, attr)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def close(self) -> None:
        self.connection.close()


def get_data(
    query: str,
    params: dict[str, Any] | None = None,
    to_lowercase_cols: bool = False,
    as_pyarrow_table: bool = False,
    verbose: bool = True,
    role: str | None = None,
    warehouse: str | None = None,
    is_production: bool = False,
) -> pd.DataFrame | pa.Table:
    """Queries Snowflake and returns the results in a single Pandas DataFrame or PyArrow Table. Snowflake stores all
    columns in caps (e.g. Referral Postgres columns like `created_at` are stored as `CREATED_AT`), so a convenience
    `to_lowercase_cols` parameter can be used for compatibility with existing workflows.

    Args:
        query (str): The SQL query to execute.
        params (dict[str, Any], optional): keys/values for binding to variables in the SQL statement using the
            pyformat `%(variable)s` style in the query string. Defaults to None.
        to_lowercase_cols (bool, optional): Convert the dataframe columns to lowercase before returning. Defaults to
            False.
        as_pyarrow_table (bool, optional): Whether the query results should be returned as a PyArrow Table. PyArrow is
            used regardless of this parameter, and is whether the results should be converted into a Pandas DataFrame.
            Keeping the results as a PyArrow Table uses ~30% of the total memory and is ~40% faster to load, largely
            due to avoiding the Pandas concatentation (PyArrow uses zero-copy reads/concats). Defaults to False.
        verbose (bool, optional): Whether to log additional serialization and memory metrics. Defaults to False.
        warehouse (str, optional): The Snowflake warehouse to use. Defaults to None (`DATASCIENCE_WH`). Other options
        may include `DATASCIENCE_WH` (Medium), `SYSTEM_DATASCIENCE_WH` (Large), and `SYSTEM_DATASCIENCE_VIP_WH`
        (X-Large).
        role (str, optional): The Snowflake role to use. Defaults to None (`PRODUCER_DATASCIENCE_ROLE`).
        is_production (bool, optional): Whether to request production snowflake credentials. Defaults to False. If sets
        to True, the `SYSTEM_DATASCIENCE_WH` will be used by Default `warehouse`.

    Returns:
        pd.DataFrame | pa.Table: containing all of the concatentated results.
    """  # noqa: D205
    with SnowflakeConnection(
        is_production=is_production,
        role=role,
        warehouse=warehouse,
    ) as con, con.cursor() as cur:
        logger.info("Executing query.")
        if params:
            logger.info(f"Using params: {params}")

        cur.execute(command=query, params=params)
        logger.info(f"Result row count: {cur.rowcount}")

        if as_pyarrow_table:
            logger.info("Loading results into PyArrow Table.")
            results = cur.fetch_arrow_all()

            if verbose:
                logger.info(f"Shape: {results.shape}")
                logger.info(f"Size in memory: {results.nbytes / 1024**3 : .3f} GB")

            if to_lowercase_cols:
                results = results.rename_columns(
                    [col.lower() for col in results.column_names]
                )
        else:
            logger.info("Loading results into Pandas DataFrame.")
            results = cur.fetch_pandas_all()

            if verbose:
                logger.info(f"Shape: {results.shape}")
                logger.info(
                    f"Size in memory: {results.memory_usage(deep=True).sum() / 1024**3 : .3f} GB"
                )

            if to_lowercase_cols:
                results.columns = results.columns.str.lower()

        return results


def get_data_result_batches(
    query: str,
    params: dict[str, Any] | None = None,
    role: str | None = None,
    warehouse: str | None = None,
    is_production: bool = False,
) -> list[snowflake.connector.result_batch.ArrowResultBatch]:
    """Queries Snowflake and returns a list of pointers to the underlying results which are stored in chunked
    parquet files
    behind the scenes. Can be used alongside `prepare_result_batch()` to replicate common access patterns from
    `get_data()` in a distributed workflow.

    This can be useful in combination with Metaflow foreach/fanout steps to distribute preprocessing in parallel. The
    result batches can be passed to subsequent steps to be materialized separately without ever joining the entire
    dataset together.
    See: https://docs.snowflake.com/en/user-guide/python-connector-distributed-fetch.html

    Args:
        query (str): The SQL query to execute.
        params (dict[str, Any], optional): keys/values for binding to variables in the SQL statement using the
            pyformat `%(variable)s` style in the query string. Defaults to None.
        is_production (bool, optional): Whether to request production snowflake credentials. Defaults to False.

    Returns:
        list[snowflake.connector.result_batch.ArrowResultBatch]: The result chunks from calling
            `get_result_batches()` on a Snowflake cursor after executing a query. Note that these chunks can be
            materialized after the connection is closed.
    """  # noqa: D205
    with SnowflakeConnection(
        is_production=is_production,
        role=role,
        warehouse=warehouse,
    ) as con, con.cursor() as cur:
        logger.info("Executing query.")
        if params:
            logger.info(f"Using params: {params}")

        cur.execute(command=query, params=params)
        result_batches = cur.get_result_batches()
        logger.info(
            f"Results chunked in {len(result_batches)} batches with total row count: {cur.rowcount}"
        )
        return result_batches


def prepare_result_batch(
    result_batch: snowflake.connector.result_batch.ArrowResultBatch,
    to_lowercase_cols: bool = False,
    as_pyarrow_table: bool = False,
    verbose: bool = False,
) -> pd.DataFrame | pa.Table:
    """Prepares an individual batch of results from the list of chunks given by `get_result_batches()` on a Snowflake
    cursor after executing a query.

    This can be useful in combination with Metaflow foreach/fanout steps to distribute preprocessing in parallel. The
    result batches can be passed to subsequent steps to be materialized separately without ever joining the entire
    dataset together.
    See: https://docs.snowflake.com/en/user-guide/python-connector-distributed-fetch.html

    Args:
        result_batch (snowflake.connector.result_batch.ArrowResultBatch): A single result chunk (batch) from calling
            `get_result_batches()` on a Snowflake cursor after executing a query.
        to_lowercase_cols (bool, optional): Convert the dataframe columns to lowercase before returning. Defaults to
            False.
        as_pyarrow_table (bool, optional): Whether the query results should be returned as a PyArrow Table. PyArrow is
            used regardless of this parameter, and is whether the results should be converted into a Pandas DataFrame.
            Keeping the results as a PyArrow Table uses ~30% of the total memory and is ~40% faster to load, largely
            due to avoiding the Pandas concatentation (PyArrow uses zero-copy reads/concats). Defaults to False.
        verbose (bool, optional): Whether to log additional serialization and memory metrics. Defaults to False.

    Returns:
        pd.DataFrame | pa.Table: containing a batch of the results.
    """  # noqa: D205
    if verbose:
        logger.info(
            f"Result batch has rowcount of {result_batch.rowcount}, "
            f"compressed size of {(result_batch.compressed_size or 0.) / 1024**3 : .3f} GB "
            f"and uncompressed size of {(result_batch.uncompressed_size or 0.) / 1024**3 : .3f} GB"
        )

    if as_pyarrow_table:
        results = result_batch.to_arrow()

        logger.info(f"Shape: {results.shape}")
        if verbose:
            logger.info(f"Size in memory: {results.nbytes / 1024**3 : .3f} GB")

        if to_lowercase_cols:
            results = results.rename_columns(
                [col.lower() for col in results.column_names]
            )

    else:
        results = result_batch.to_pandas()

        logger.info(f"Shape: {results.shape}")
        if verbose:
            logger.info(
                f"Size in memory: {results.memory_usage(deep=True).sum() / 1024**3 : .3f} GB"
            )

        if to_lowercase_cols:
            results.columns = results.columns.str.lower()

    return results


def get_data_batches(
    query: str,
    params: dict[str, Any] | None = None,
    to_lowercase_cols: bool = False,
    as_pyarrow_table: bool = False,
    verbose: bool = False,
) -> Iterator[pd.DataFrame | pa.Table]:
    """Generator that yields materialized Pandas DataFrames or PyArrow Tables of subsets of the Snowflake
    query result set.
    The size of each batch is controlled by Snowflake and cannot be configured.

    This can be useful when subsets of the data can be operated on independently and/or in combination with Metaflow
    foreach/fanout steps to distribute preprocessing in parallel.

    Args:
        query (str): The SQL query to execute.
        params (dict[str, Any], optional): keys/values for binding to variables in the SQL statement using the
            pyformat `%(variable)s` style in the query string. Defaults to None.
        to_lowercase_cols (bool, optional): Convert the dataframe columns to lowercase before returning. Defaults to
            False.
        as_pyarrow_table (bool, optional): Whether the query results should be returned as a PyArrow Table. PyArrow is
            used regardless of this parameter, and is whether the results should be converted into a Pandas DataFrame.
            Keeping the results as a PyArrow Table uses ~30% of the total memory and is ~40% faster to load, largely
            due to avoiding the Pandas concatentation (PyArrow uses zero-copy reads/concats). Defaults to False.
        verbose (bool, optional): Whether to log per-batch serialization and memory metrics. Defaults to False.

    Yields:
        pd.DataFrame | pa.Table: containing a batch of the results.
    """  # noqa: D205
    result_batches = get_data_result_batches(query=query, params=params)
    num_batches = len(result_batches)

    for idx, result_batch in enumerate(result_batches, start=1):
        logger.info(f"Loading batch {idx} of {num_batches}.")

        results = prepare_result_batch(
            result_batch=result_batch,
            to_lowercase_cols=to_lowercase_cols,
            as_pyarrow_table=as_pyarrow_table,
            verbose=verbose,
        )

        yield results


def write_data(
    df: pd.DataFrame,
    database: str | None = "USER_SANDBOX",
    schema: str | None = "PUBLIC",
    tablename: str | None = None,
    auto_create_table: bool = True,
    overwrite: bool = False,
    is_production: bool = False,
    role: str | None = None,
    warehouse: str | None = None,
    write_datetime_to_string: bool = True,
    chunk_size: int = 100000,
    compression: str = "gzip",
    parallel: int = 4,
    **kwargs: Any,
) -> tuple[
    bool,
    int,
    int,
    Sequence[
        tuple[
            str,
            str,
            int,
            int,
            int,
            int,
            str | None,
            int | None,
            int | None,
            str | None,
        ]
    ],
]:
    """Writes a Pandas DataFrame to a Snowflake table. Note that a table name
    such as "USER_SANDBOX.USERNAME.TABLE_NAME", must be explicitly passed as
    database="USER_SANDBOX", schema="USERNAME", tablename="TABLE_NAME".

    Args:
        df (pandas.DataFrame):
            The DataFrame to be written to Snowflake.
        database (str, optional):
            The Snowflake database name. If provided, it will be converted to uppercase.
            Defaults to 'USER_SANDBOX'.
        schema (str, optional):
            The Snowflake schema name. If provided, it will be converted to uppercase.
            Defaults to 'PUBLIC'.
        tablename (str):
            The name of the Snowflake table to write the DataFrame to. It must be provided,
            and it will be converted to uppercase.
        auto_create_table (bool):
            When true, will automatically create a table with corresponding columns for each
            column in the passed in DataFrame. The table will not be created if it already exists
        overwrite (bool):
            When true, and if auto_create_table is true, then it drops the table. Otherwise, it
            truncates the table. In both cases it will replace the existing contents of the table
            with that of the passed in Pandas DataFrame.
        is_production (bool, optional):
            Whether to request production snowflake credentials. Defaults to False.
        write_datetime_to_string (bool, optional):
            Converts datetime columns to strings. This is particularly useful when working with custom
            datetime formats (e.g., dates, timezone-encoded data). Setting this to false, may entail some
            up-stream data transformations for desired results. Defaults to True.
        chunk_size: Number of elements to be inserted once, if not provided all elements will be dumped once
            Defaults to None.
        compression: The compression used on the Parquet files, can only be gzip, or snappy. Gzip gives supposedly a
            better compression, while snappy is faster. Use whichever is more appropriate. Defaults to 'gzip'.
        parallel: Number of threads to be used when uploading chunks, default follows documentation at:
            https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters. Defaults to 4.


    Returns:
        Returns the COPY INTO command's results to verify ingestion in the form of a tuple of whether all chunks were
        ingested correctly, # of chunks, # of ingested rows, and ingest's output.


    Note:
        - This function ensures Snowflake compatibility by converting column names to uppercase in the
          DataFrame to prevent issues with case sensitivity.
        - The function connects to Snowflake using the provided database and schema, and writes the DataFrame
          to the specified table with the option to auto-create the table if it does not exist.
    """  # noqa: D205
    write_df = df.copy()

    # Step below required to ensure Snowflake-compatibility.
    # In the absence of this step, quotation marks are added around the name of
    # the columns, which are treated as case-sensitive.
    write_df.columns = write_df.columns.str.upper()

    # The snowflake API can mismanage the formatting of datetimes,
    # particularly for just dates, or custom timeformats (with specific timezones for e.g.)
    # The step below serves as a workaround to address these concerns
    if write_datetime_to_string:
        for col in write_df.select_dtypes(include=["datetime"]).columns:
            write_df[col] = write_df[col].astype(str)

    # Due to snowflake compatibility issues,the database, schema and table should be uppercase
    database = database.upper() if database else None
    schema = schema.upper() if schema else None
    tablename = tablename.upper() if tablename else None

    if tablename is None:
        logger.info("Missing tablename.")
        return (False, None, None, None)

    with SnowflakeConnection(
        database=database,
        schema=schema,
        role=role,
        warehouse=warehouse,
        is_production=is_production,
        # case-sensitive, case-sensitive,case-sensitive,case-sensitive, boolean
    ) as con, con.cursor():
        success, nchunks, nrows, ingest_output = write_pandas(
            conn=con,
            df=write_df,
            table_name=tablename,
            schema=schema,  # case-sensitive
            database=database,  # case-sensitive
            auto_create_table=auto_create_table,  # creates table if one does not exist, unless overwrite=true
            overwrite=overwrite,  # Replaces existing table if both this and auto_create_table are true
            chunk_size=chunk_size,
            compression=compression,
            parallel=parallel,
            **kwargs,
        )
    return (success, nchunks, nrows, ingest_output)
