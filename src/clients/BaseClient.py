from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Union
import polars as pl
import geopandas as gpd

TemporalFilter = Optional[Union[datetime, tuple[datetime, datetime]]]
SpatialFilter = Optional[gpd.GeoSeries]


class BaseClient(ABC):
    @abstractmethod
    def get_data(
        self,
        spatial_filter: Optional[gpd.GeoSeries] = None,
        temporal_filter: Optional[Union[datetime, tuple[datetime, datetime]]] = None,
    ) -> pl.DataFrame:
        """Fetch data with optional spatial and temporal filtering

        Args:
            spatial_filter: GeoSeries containing one or more geometries to filter by
            temporal_filter: Single datetime for point-in-time or tuple of datetimes for range

        Returns:
            DataFrame containing the filtered data
        """
        pass
