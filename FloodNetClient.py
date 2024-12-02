import logging
from typing import Any, Dict, Iterable

import geopandas as gpd
from shapely.geometry.base import BaseGeometry
import polars as pl
import requests
from airflow.providers.http.hooks.http import HttpHook

logger = logging.getLogger(__name__)


class DirectHttpHook:
    """Simple hook that makes HTTP requests directly without Airflow"""

    def __init__(
        self,
        method: str = "GET",
        base_url: str = "https://api.dev.floodlabs.nyc/api/rest/",
    ):
        self.method = method
        self.base_url = base_url

    def run(self, endpoint: str, params: Dict[str, Any] = None) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        return requests.request(self.method, url, params=params)


class FloodNetClient:
    """Client for fetching and processing FloodNet data"""

    def __init__(self, http_conn_id: str = "floodnet_default"):
        logger.info(f"Initializing FloodDataClient with connection ID: {http_conn_id}")
        self.hook = HttpHook(method="GET", http_conn_id=http_conn_id)

    def get_deployments(self) -> pl.DataFrame:
        """Get and process deployment data"""
        logger.info("Fetching deployment data from API")
        try:
            response = self.hook.run("deployments/flood")
            logger.info(f"API response received: status {response.status_code}")
            return self._process_deployments(response.json())
        except Exception as e:
            logger.error(f"Error fetching deployments: {str(e)}")
            raise

    @staticmethod
    def st_filter_deployments_within(
        deployments: pl.DataFrame, bounds: gpd.GeoDataFrame
    ) -> pl.DataFrame:
        """Filter deployment data to only those within given polygon layer"""
        logger.info(f"Filtering deployments within {len(bounds)} bounds")

        deployments_gdf = gpd.GeoDataFrame(
            deployments.to_pandas(),
            geometry=gpd.points_from_xy(deployments.lon, deployments.lat),
            crs="EPSG:4326",
        )

        if bounds.crs is None:
            logger.warning("Bounds CRS not set, assuming EPSG:4326")
            bounds = bounds.set_crs("EPSG:4326")
        elif bounds.crs != "EPSG:4326":
            logger.info(f"Converting bounds from {bounds.crs} to EPSG:4326")
            bounds = bounds.to_crs("EPSG:4326")

        bounds_union: BaseGeometry = bounds.union_all()
        filtered = deployments_gdf[deployments_gdf.within(bounds_union)]
        return pl.from_pandas(filtered.drop(columns=["geometry"]))

    @staticmethod
    def get_active_filter(as_of_time: str) -> pl.Expr:
        """Create filter expression for active deployments"""
        return (
            pl.col("date_deployed").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ")
            <= pl.lit(as_of_time).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ")
        ) & (pl.col("sensor_status") == "good")

    @staticmethod
    def _process_deployments(data: Dict[str, Any]) -> pl.DataFrame:
        """Transform raw deployment data into structured DataFrame"""
        breakpoint()
        try:
            deployments = pl.DataFrame(data[0])
            logger.debug(f"Initial deployments shape: {deployments.shape}")

            coordinates = deployments.select(
                pl.col("location").struct.field("coordinates")
            ).to_series()

            processed = deployments.with_columns(
                [
                    pl.Series("lon", [coord[0] for coord in coordinates]),
                    pl.Series("lat", [coord[1] for coord in coordinates]),
                ]
            ).drop("location")

            logger.info(f"Processed {len(processed)} deployment records")
            return processed
        except Exception as e:
            logger.error(f"Error processing deployments data: {str(e)}")
            raise

    def get_deployment_depth(
        self, deployment_id: str, start_time: str, end_time: str
    ) -> pl.DataFrame:
        """
        Fetch depth data for a single deployment within a specified time range.

        Args:
            deployment_id: The ID of the deployment to query
            start_time: Start time in ISO format
            end_time: End time in ISO format

        Returns:
            DataFrame containing depth readings or empty DataFrame if no data found
        """
        logger.debug(f"Querying depth data for deployment {deployment_id}")
        try:
            response = self.hook.run(
                f"deployments/flood/{deployment_id}/depth",
                params={"start_time": start_time, "end_time": end_time},
            )

            depth_data = pl.DataFrame(response.json())
            if len(depth_data) > 0:
                logger.debug(
                    f"Got {len(depth_data)} readings for deployment {deployment_id}"
                )
                return depth_data.with_columns(
                    pl.col("time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ")
                )
            else:
                logger.debug(f"No depth data found for deployment {deployment_id}")
                return pl.DataFrame()

        except Exception as e:
            logger.error(f"Error querying deployment {deployment_id}: {str(e)}")
            return pl.DataFrame()

    def fetch_depth_data(
        self, deployment_ids: Iterable[str], start_time: str, end_time: str
    ) -> pl.DataFrame:
        """Fetch and combine depth data for multiple deployments"""
        n_deployments = len(deployment_ids)
        logger.info(f"Fetching depth data for {n_deployments} deployments")

        depth_data_list = []
        for deployment_id in deployment_ids:
            depth_data = self.get_single_deployment_depth(
                deployment_id, start_time, end_time
            )
            if not depth_data.is_empty():
                depth_data_list.append(depth_data)

        if not depth_data_list:
            logger.warning("No depth data found for any deployments")
            return pl.DataFrame()

        combined = pl.concat(depth_data_list)
        logger.info(
            f"Combined {len(combined)} depth readings from {len(depth_data_list)} deployments"
        )
        return combined
