# NYC Flood Data Pipeline

A comprehensive data pipeline for collecting and managing flooding-related data in New York City. This project consolidates data from multiple sources including:

- [FloodNet](https://www.floodnet.nyc/) sensor network
- Micronet rain gauges
- Weather data
- 311 flooding-related service requests

The pipeline provides tools for automated data collection, validation, and storage of flood-related measurements and reports.

## 🚧 Work in Progress

This project is under active development. Core functionality is being implemented and tested.

## Features

- Automated collection from multiple flooding-related data sources
- Integration of sensor networks, weather data, and public service requests
- Spatiotemporal filtering capabilities for data queries
- PostGIS-enabled database schema for geospatial data storage
- Apache Airflow DAGs for scheduled data collection
- Data validation framework
- Polars-based data processing for efficient handling of time-series data

## Architecture

- **Data Collection**: Python clients for multiple data sources including:
  - FloodNet REST API
  - Micronet rain gauge data
  - Weather data APIs
  - NYC 311 service requests
- **Storage**: PostgreSQL/PostGIS database for structured storage of flooding-related data
- **Orchestration**: Apache Airflow for workflow management
- **Processing**: Polars and GeoPandas for efficient data manipulation

## Getting Started

Documentation and setup instructions coming soon.

## License

TBD
