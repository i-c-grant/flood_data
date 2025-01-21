"""Simple client for interacting with the FloodNet API.

A lightweight client that uses Pydantic for validation and requests for HTTP.
Provides core functionality for:
- Fetching deployment locations 
- Retrieving time-series depth measurements
- Basic temporal filtering
"""

import logging
from datetime import datetime
from typing import List, Optional

import requests

from src.clients.schemas import Deployment, DeploymentResponse, DepthReading, DepthResponse

logger = logging.getLogger(__name__)

API_BASE: str = "https://api.dev.floodlabs.nyc/api/rest/"

class FloodNetClient:
    """Lightweight client for fetching FloodNet sensor data"""
    
    def __init__(self):
        self.base_url = API_BASE
        
    def get_deployments(self) -> List[Deployment]:
        """Get all deployment locations and metadata.
        
        Returns:
            List of validated Deployment models with processed coordinates
        """
        logger.info("Fetching deployment data from API")
        try:
            response = requests.get(f"{self.base_url}deployments/flood")
            response.raise_for_status()
            
            # Parse and validate response directly from JSON
            try:
                data = DeploymentResponse.model_validate_json(response.text)
            except ValueError as e:
                logger.error("Invalid JSON response: %s", str(e))
                raise
            
            # Process coordinates for each deployment
            for deployment in data.deployments:
                deployment.longitude = deployment.location.coordinates[0]
                deployment.latitude = deployment.location.coordinates[1]
                
            logger.info("Processed %d deployment records", len(data.deployments))
            return data.deployments
            
        except Exception as e:
            logger.error("Error fetching deployments: %s", str(e))
            raise

    def get_deployment_ids(self) -> List[str]:
        """Get a list of all deployment IDs.
        
        Returns:
            List of deployment IDs
        """
        deployments = self.get_deployments()
        return [d["deployment_id"] for d in deployments]

    def get_depth_data(
        self,
        start_time: datetime,
        end_time: datetime,
        deployment_ids: Optional[List[str]] = None
    ) -> List[DepthReading]:
        """Fetch depth data for specified deployments.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            deployment_ids: Optional list of specific deployment IDs to query.
                          If None, queries all deployments.
                          
        Returns:
            List of validated DepthReading models
        """
        # If no deployment IDs provided, get all deployments
        if deployment_ids is None:
            deployments = self.get_deployments()
            deployment_ids = [d.deployment_id for d in deployments]
            
        logger.info("Fetching depth data for %d deployments", len(deployment_ids))
        
        params = {
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        
        all_readings = []
        for dep_id in deployment_ids:
            try:
                response = requests.get(
                    f"{self.base_url}deployments/flood/{dep_id}/depth",
                    params=params
                )
                response.raise_for_status()
                
                # Parse and validate response directly from JSON
                try:
                    depth_response = DepthResponse.model_validate_json(response.text)
                except ValueError as e:
                    logger.error("Invalid JSON response for deployment %s: %s", dep_id, str(e))
                    continue
                logger.debug("Got %d readings for deployment %s", 
                            len(depth_response.depth_data), dep_id)
                all_readings.extend(depth_response.depth_data)
                
            except Exception as e:
                logger.error("Error querying deployment %s: %s", dep_id, str(e))
                continue
            
        logger.info("Retrieved %d total depth readings", len(all_readings))
        return all_readings
