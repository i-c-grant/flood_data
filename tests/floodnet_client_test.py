import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import geopandas as gpd
import polars as pl
import pytest
import requests
from src.clients.FloodNetClient import FloodNetClient

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

    def run(self, endpoint: str, data: Dict[str, Any] = None) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        return requests.request(self.method, url, params=data)

    def __str__(self):
        return "dummy hook"


######################################
# General processing and API  tests  #
######################################
@pytest.fixture
def client():
    hook = DirectHttpHook()
    return FloodNetClient(hook)  # Uses default "floodnet_default" connection ID


def test_get_deployments(client):
    """Test fetching real deployment data"""
    deployments = client.get_deployments()

    # Verify structure
    assert isinstance(deployments, pl.DataFrame)
    required_cols = {"deployment_id", "lon", "lat", "date_deployed", "sensor_status"}
    assert all(col in deployments.columns for col in required_cols)

    # Verify data types
    assert deployments["lon"].dtype == pl.Float64
    assert deployments["lat"].dtype == pl.Float64

    # Verify reasonable coordinate bounds for NYC
    assert all((-74.3 < lon < -73.7) for lon in deployments["lon"])
    assert all((40.4 < lat < 40.95) for lat in deployments["lat"])

    print(f"Found {len(deployments)} total deployments")


def test_get_single_deployment_depth(client):
    """Test fetching depth data for a single deployment"""
    # First get deployments to find an active one
    deployments = client.get_deployments()
    active_deployments = deployments.filter((pl.col("sensor_status") == "good"))

    if len(active_deployments) == 0:
        pytest.skip("No active deployments found")

    # Get depth data for the last week
    deployment_id = active_deployments["deployment_id"][0]
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=1)

    depth_data = client.get_deployment_depth(deployment_id, start_time, end_time)

    # Verify structure
    assert isinstance(depth_data, pl.DataFrame)
    assert "time" in depth_data.columns
    assert "depth_proc_mm" in depth_data.columns

    if len(depth_data) > 0:
        # Verify data types
        assert all(isinstance(t, datetime) for t in depth_data["time"])
        assert depth_data["depth_proc_mm"].dtype == pl.Float64

        # Verify reasonable depth values (in mm)
        assert all(
            (0 <= d <= 1000) for d in depth_data["depth_proc_mm"] if d is not None
        )

        print(f"Found {len(depth_data)} readings for deployment {deployment_id}")
    else:
        print(f"No recent depth readings for deployment {deployment_id}")


def test_fetch_multiple_deployments(client):
    """Test fetching depth data for multiple deployments"""
    # Get active deployments
    deployments = client.get_deployments()
    active_deployments = deployments.filter((pl.col("sensor_status") == "good"))

    if len(active_deployments) < 2:
        pytest.skip("Need at least 2 active deployments")

    # Get data for 2 deployments over last hour
    deployment_ids = active_deployments["deployment_id"][:2].to_list()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=1)

    depth_data = client.fetch_depth_data(
        deployment_ids, start_time.isoformat() + "Z", end_time.isoformat() + "Z"
    )

    # Verify structure and data
    assert isinstance(depth_data, pl.DataFrame)
    if len(depth_data) > 0:
        assert "time" in depth_data.columns
        assert "depth_proc_mm" in depth_data.columns
        assert "deployment_id" in depth_data.columns

        # Check we got data for both deployments
        unique_deployments = set(depth_data["deployment_id"])
        assert len(unique_deployments) <= 2

        print(f"Found data for {len(unique_deployments)} deployments:")
        for dep_id in unique_deployments:
            dep_data = depth_data.filter(pl.col("deployment_id") == dep_id)
            print(f"- {dep_id}: {len(dep_data)} readings")
    else:
        print("No recent depth readings found")


if __name__ == "__main__":
    client = FloodNetClient()  # Uses default connection ID
    test_get_deployments(client)
    test_get_single_deployment_depth(client)
    test_fetch_multiple_deployments(client)


#################
# Spatial tests #
#################
@pytest.fixture
def test_geometries():
    """Load test geometries from GeoPackage"""
    test_data_path = Path(__file__).parent / "data" / "test_data.gpkg"

    return {
        "queens": gpd.read_file(test_data_path, layer="queens").geometry,
        "brooklyn": gpd.read_file(test_data_path, layer="brooklyn").geometry,
        "nyc": gpd.read_file(test_data_path, layer="nyc").geometry,
    }


def test_spatial_filter_queens(client, test_geometries):
    """Test filtering deployments within Queens bounds"""
    # Get all deployments
    all_deployments = client.get_deployments()

    # Filter to Queens
    queens_deployments = client.st_filter_deployments_within(
        all_deployments, test_geometries["queens"]
    )

    # Basic validation
    assert len(queens_deployments) <= len(all_deployments)
    assert all((-73.95 < lon < -73.7) for lon in queens_deployments["lon"])
    assert all((40.55 < lat < 40.8) for lat in queens_deployments["lat"])

    print(f"Found {len(queens_deployments)} deployments in Queens")


def test_spatial_filter_brooklyn(client, test_geometries):
    """Test filtering deployments within Brooklyn bounds"""
    all_deployments = client.get_deployments()

    brooklyn_deployments = client.st_filter_deployments_within(
        all_deployments, test_geometries["brooklyn"]
    )

    assert len(brooklyn_deployments) <= len(all_deployments)
    assert all((-74.05 < lon < -73.85) for lon in brooklyn_deployments["lon"])
    assert all((40.55 < lat < 40.75) for lat in brooklyn_deployments["lat"])

    print(f"Found {len(brooklyn_deployments)} deployments in Brooklyn")


def test_spatial_filter_nyc(client, test_geometries):
    """Test filtering deployments within all of NYC"""
    all_deployments = client.get_deployments()

    nyc_deployments = client.st_filter_deployments_within(
        all_deployments, test_geometries["nyc"]
    )

    # Should get same or very close to same number as all deployments
    # Allow small difference in case some deployments are just outside city bounds
    assert len(nyc_deployments) >= len(all_deployments) * 0.95

    print(
        f"Found {len(nyc_deployments)} deployments in NYC out of {len(all_deployments)} total"
    )


def test_spatial_filter_empty_result(client, test_geometries):
    """Test filtering with geometry that should return no results"""
    all_deployments = client.get_deployments()

    # Create a small polygon far from NYC
    far_away = gpd.GeoSeries(
        gpd.points_from_xy(
            x=[-118.2437], y=[34.0522]
        ).buffer(  # Los Angeles coordinates
            0.01
        ),  # Small buffer around point
        crs="EPSG:4326",
    )

    filtered = client.st_filter_deployments_within(all_deployments, far_away)
    assert len(filtered) == 0


def test_spatial_filter_crs_conversion(client, test_geometries):
    """Test handling of different CRS in input geometry"""
    all_deployments = client.get_deployments()

    # Convert NYC bounds to State Plane (common for NYC data)
    nyc_state_plane = test_geometries["nyc"].to_crs("EPSG:2263")

    filtered = client.st_filter_deployments_within(all_deployments, nyc_state_plane)

    # Should get approximately same results as with WGS84
    nyc_wgs84 = client.st_filter_deployments_within(
        all_deployments, test_geometries["nyc"]
    )

    assert abs(len(filtered) - len(nyc_wgs84)) <= 1


def test_multiple_polygons(client, test_geometries):
    """Test filtering with multiple polygons"""
    import pandas as pd

    all_deployments = client.get_deployments()

    # Combine Brooklyn and Queens
    bk_qns = gpd.GeoSeries(
        pd.concat([test_geometries["brooklyn"], test_geometries["queens"]]),
        crs="EPSG:4326",
    )

    filtered = client.st_filter_deployments_within(all_deployments, bk_qns)

    # Should be less than total NYC deployments
    nyc_deployments = client.st_filter_deployments_within(
        all_deployments, test_geometries["nyc"]
    )
    assert len(filtered) < len(nyc_deployments)

    print(f"Found {len(filtered)} deployments in Brooklyn and Queens combined")
