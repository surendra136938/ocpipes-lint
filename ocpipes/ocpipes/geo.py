from typing import Tuple, Union

import geopandas as gp
import pandas as pd

from ocpipes.db import get_engine


def zips_by_market_id(market_id: Union[int, str]) -> gp.GeoDataFrame:
    """
    Query the zipcodes of a market.

    Args:
        market_id (int | str): market to query.
    Returns:
        gp.GeoDataFrame: Columns for `zipcode`, `market_name`, `state`, `geom` in the market.
    """
    market_zips_query = """
    select
        z.zipcode as zipcode
        , m.name as market_name
        , m.state as state
        , z.geom as geom
    from zipcode z
        inner join market m on z.market_id = m.id
    where market_id = %(market_id)s
    """
    engine = get_engine()
    market_zips = gp.GeoDataFrame.from_postgis(market_zips_query, engine, params={"market_id": market_id})
    return market_zips[pd.to_numeric(market_zips["zipcode"], errors="coerce").notnull()]


def get_market_and_zips(market_id: Union[int, str]) -> Tuple[str, gp.GeoDataFrame, str]:
    """
    Gets the zipcodes of a market, sanitizes the zip geometry, and calculates the centroid.

    Args:
        market_id (int | str): market to query.
    Returns:
        str: Normalized market name to lowercase "city_state".
        gp.GeoDataFrame: Columns for `zipcode`, `market_name`, `state`, `geom`, `centroid_lon`, `centroid_lat`.
        str: Market name as defined by the market table.
    """
    market_zips = zips_by_market_id(market_id)

    # Casting from multipolygon to polygon, if there's more than one polygon in the geometry just take the largest by area to be safe.
    for idx, zip_row in market_zips.iterrows():
        max_geom = sorted(list(zip_row["geom"].geoms), reverse=True, key=lambda x: x.area)[0]
        market_zips.at[idx, "geom"] = max_geom

    market_zips["centroid_long"] = market_zips["geom"].centroid.x
    market_zips["centroid_lat"] = market_zips["geom"].centroid.y
    market_name = market_zips.market_name.unique()[0]
    normalized_market_name = market_name.strip().lower().replace(",", "").replace(" ", "_")

    return normalized_market_name, market_zips, market_name
