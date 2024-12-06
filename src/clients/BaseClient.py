"""Base client interface for data source access with spatiotemporal filtering."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Union, Tuple

import geopandas as gpd
import polars as pl

TemporalFilter = Optional[Union[datetime, Tuple[datetime, datetime]]]
SpatialFilter = Optional[gpd.GeoSeries]


class BaseClient(ABC):
    """Abstract base client for fetching filtered data from a source."""
    @abstractmethod
    def get_data(
        self,
        spatial_filter: SpatialFilter = None,
        temporal_filter: TemporalFilter = None,
    ) -> pl.DataFrame:
        """Fetch data with optional spatial and temporal filtering

        Args:
            spatial_filter: GeoSeries containing one or more geometries to filter by
            temporal_filter: Single datetime for point-in-time or tuple of datetimes for range

        Returns:
            DataFrame containing the filtered data
        """
        pass
