"""Client for interacting with the FloodNet API to fetch flood sensor data.

This module provides a client implementation for accessing the FloodNet sensor network.
It supports:

- Fetching deployment locations and metadata
- Retrieving time-series depth measurements
- Spatial filtering of deployments by geographic bounds
- Temporal filtering of data by time range or point-in-time

The client implements the BaseClient interface.
"""

import logging
import warnings
from datetime import datetime
from typing import Any, Dict, Iterable

import geopandas as gpd
import polars as pl
import requests
from shapely.geometry.base import BaseGeometry
from src.clients.BaseClient import BaseClient, SpatialFilter, TemporalFilter

logger = logging.getLogger(__name__)

API_BASE: str = "https://api.dev.floodlabs.nyc/api/rest/"

class DirectHttpHook:
    """Simple hook that makes HTTP requests directly without Airflow"""

    def __init__(
        self,
            base_url: str,
            method: str = "GET",
    ):
        self.method = method
        self.base_url = base_url

    def run(self, endpoint: str, data: Dict[str, Any] = None) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        return requests.request(self.method, url, params=data)

    def __str__(self):
        return f"DirectHttpHook(method={self.method}, base_url={self.base_url})"


class FloodNetClient(BaseClient):
    """Client for fetching and processing FloodNet data"""

    def __init__(self, hook: DirectHttpHook):
        logger.info("Initializing FloodDataClient with connection ID: %s", hook)
        self.hook = hook

    def get_data(
        self,
        spatial_filter: SpatialFilter = None,
        temporal_filter: TemporalFilter = None,
    ) -> pl.DataFrame:
        """
        Fetch FloodNet data with optional spatial and temporal filtering.

        Args:
            spatial_filter: GeoSeries containing one or more geometries to filter by
            temporal_filter: Single datetime for point-in-time or tuple of datetimes for range

        Returns:
            DataFrame containing the filtered flood depth data
        """
        logger.info("Fetching FloodNet data with filters")

        # Get deployments data first
        deployments: pl.DataFrame = self.get_deployments()

        # Apply spatial filter to deployments if provided
        if spatial_filter is not None:
            deployments = self.st_filter_deployments_within(deployments, spatial_filter)
            if deployments.is_empty():
                logger.info("No deployments found within spatial filter")
                return pl.DataFrame()

        # If end date provided, filter deployments to only those active by that time
        if isinstance(temporal_filter, tuple):
            deployments = deployments.filter(
                self.get_active_deployment_filter(temporal_filter[1])
            )

        # Make API request, with temporal filter if provided
        if temporal_filter is not None:
            if isinstance(temporal_filter, tuple):
                start_time, end_time = temporal_filter
            else:
                start_time = end_time = temporal_filter

            depth_data = self.fetch_depth_data(
                deployments["deployment_id"], start_time, end_time
            )

            if depth_data.is_empty():
                logger.info("No depth data found for filtered deployments")
                return pl.DataFrame()

            result = depth_data.join(
                deployments.select(["deployment_id", "lon", "lat", "name"]),
                on="deployment_id",
                how="left",
            )

            return result
        else:
            # Without temporal filter, return just the deployments data
            warnings.warn("No temporal filter provided, returning deployments only")
            return deployments

    def get_deployments(self) -> pl.DataFrame:
        """Get and process deployment data"""
        logger.info("Fetching deployment data from API")
        try:
            response = self.hook.run("deployments/flood")
            logger.info("API response received: status %d", response.status_code)
            return self._process_deployments(response.json())
        except Exception as e:
            logger.error("Error fetching deployments: %s", str(e))
            raise

    @staticmethod
    def create_hook() -> DirectHttpHook:
        """Create an HTTP hook configured for the FloodNet API."""
        hook = DirectHttpHook(method="GET", base_url=API_BASE)
        return hook

    @staticmethod
    def st_filter_deployments_within(
        deployments: pl.DataFrame, bounds: gpd.GeoSeries
    ) -> pl.DataFrame:
        """Filter deployments to those within specified geographic bounds.
        
        Args:
            deployments: DataFrame containing deployment locations
            bounds: GeoSeries of polygons defining filter area
            
        Returns:
            DataFrame of deployments within bounds
        """
        logger.info("Filtering deployments within %d bounds", len(bounds))

        deployments_gdf = gpd.GeoDataFrame(
            deployments.to_pandas(),
            geometry=gpd.points_from_xy(deployments["lon"], deployments["lat"]),
            crs="EPSG:4326",
        )

        if bounds.crs is None:
            logger.warning("Bounds CRS not set, assuming EPSG:4326")
            bounds = bounds.set_crs("EPSG:4326")
        elif bounds.crs != "EPSG:4326":
            logger.info("Converting bounds from %s to EPSG:4326", bounds.crs)
            bounds = bounds.to_crs("EPSG:4326")

        bounds_union: BaseGeometry = bounds.union_all()
        filtered = deployments_gdf[deployments_gdf.within(bounds_union)]
        return pl.from_pandas(filtered.drop(columns=["geometry"]))

    @staticmethod
    def get_active_deployment_filter(active_by: datetime) -> pl.Expr:
        """Create filter expression for active deployments.
        
        Args:
            active_by: Datetime to check deployment status against
            
        Returns:
            Polars expression filtering for deployments installed by given time
        """
        return pl.col("date_deployed") <= active_by

    @staticmethod
    def _process_deployments(data: Dict[str, Any]) -> pl.DataFrame:
        """Transform raw deployment API response into structured DataFrame.
        
        Extracts coordinates and converts timestamps from the raw JSON response.
        
        Args:
            data: Raw API response dictionary
            
        Returns:
            DataFrame with processed deployment records
        """

        try:
            deployments = pl.DataFrame(data["deployments"])
            logger.debug("Initial deployments shape: %s", deployments.shape)

            # Add separate lon/lat columns
            processed_deployments = deployments.with_columns(
                pl.col("location").struct.field("coordinates").alias("coords"),
                pl.col("date_deployed").str.to_datetime(time_zone = "UTC"),
            ).select(
                pl.all().exclude("location", "coords"),
                pl.col("coords").list.first().alias("lon"),
                pl.col("coords").list.last().alias("lat"),
            )

            logger.info("Processed %d deployment records", len(processed_deployments))
            return processed_deployments
        except Exception as e:
            logger.error("Error processing deployments data: %s", str(e))
            raise

    def get_deployment_depth(
        self, deployment_id: str, start_time: datetime, end_time: datetime
    ) -> pl.DataFrame:
        """
        Fetch depth data for a single deployment within a specified time range.

        Args:
            deployment_id: The ID of the deployment to query
            start_time: Start time as a Python datetime object
            end_time: End time as a Python datetime object

        Returns:
            DataFrame containing depth readings or empty DataFrame if no data found
        """
        logger.debug("Querying depth data for deployment %s", deployment_id)
        try:
            response = self.hook.run(
                f"deployments/flood/{deployment_id}/depth",
                data={
                    "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            )

            depth_data = pl.from_dicts(
                response.json()["depth_data"],
                schema={
                    "deployment_id": pl.String,
                    "time": pl.String,
                    "depth_proc_mm": pl.Float64,
                },
            ).with_columns(pl.col("time").str.to_datetime())

            if len(depth_data) > 0:
                logger.debug(
                    "Got %d readings for deployment %s",
                    len(depth_data),
                    deployment_id
                )

                return depth_data
            else:
                logger.debug("No depth data found for deployment %s", deployment_id)
                return pl.DataFrame()

        except Exception as e:
            logger.error("Error querying deployment %s: %s", deployment_id, str(e))
            return pl.DataFrame()

    def fetch_depth_data(
        self, deployment_ids: Iterable[str], start_time: datetime, end_time: datetime
    ) -> pl.DataFrame:
        """Fetch and combine depth data for multiple deployments"""
        n_deployments = len(deployment_ids)
        logger.info("Fetching depth data for %d deployments", n_deployments)

        depth_data_list = []
        for deployment_id in deployment_ids:
            depth_data = self.get_deployment_depth(deployment_id, start_time, end_time)
            if not depth_data.is_empty():
                depth_data_list.append(depth_data)

        if not depth_data_list:
            logger.warning("No depth data found for any deployments")
            return pl.DataFrame()

        combined = pl.concat(depth_data_list)
        logger.info(
            "Combined %d depth readings from %d deployments",
            len(combined),
            len(depth_data_list)
        )
        return combined
