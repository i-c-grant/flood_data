#!/usr/bin/env python
from datetime import datetime, timedelta, timezone

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.clients.FloodNetClient import DirectHttpHook, FloodNetClient, API_BASE

def main():
    hook = DirectHttpHook(API_BASE)
    client = FloodNetClient(hook)
    pg_hook = PostgresHook(postgres_conn_id='postgres_results')
    
    # Process deployments
    deployments = client.get_deployments()
    if not deployments.is_empty():
        insert_query = """
        INSERT INTO data.floodnet_deployments 
            (deployment_id, sensor_name, sensor_status, date_deployed, location)
        VALUES 
            (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        ON CONFLICT (deployment_id) 
        DO UPDATE SET
            sensor_name = EXCLUDED.sensor_name,
            sensor_status = EXCLUDED.sensor_status,
            date_deployed = EXCLUDED.date_deployed,
            location = EXCLUDED.location;
        """

        deployment_records = [
            (row['deployment_id'], 
             row['name'], 
             row['sensor_status'],
             row['date_deployed'],
             row['lon'],
             row['lat'])
            for row in deployments.iter_rows(named=True)
        ]
        
        # pg_hook.insert_rows(
        #     table='data.floodnet_deployments',
        #     rows=deployment_records,
        #     target_fields=['deployment_id', 'sensor_name', 'sensor_status', 
        #                  'date_deployed', 'lon', 'lat'],
        #     commit_every=1000,
        #     replace=True
        # )

    # Process readings
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=5)
    readings = client.get_data(temporal_filter=(start_time, end_time))
    
    if not readings.is_empty():
        # insert_query = """
        # INSERT INTO data.floodnet_readings 
            # (deployment_id, time, depth_proc_mm)
        # VALUES 
            # (%s, %s, %s)
        # ON CONFLICT (deployment_id, time) 
        # DO UPDATE SET
            # depth_proc_mm = EXCLUDED.depth_proc_mm;
        # """
        
        reading_records = [
            (row['deployment_id'], row['time'], row['depth_proc_mm'])
            for row in readings.iter_rows(named=True)
        ]

        # pg_hook.insert_rows(
        #     table='data.floodnet_readings',
        #     rows=reading_records,
        #     target_fields=['deployment_id', 'time', 'depth_proc_mm'],
        #     commit_every=1000,
        #     replace=True
        # )
        
        # pg_hook.run(
            # insert_query,
            # parameters=reading_records,
            # commit_every=1000
        # )
    
    print(f"Processed {len(readings) if not readings.is_empty() else 0} readings")

if __name__ == "__main__":
    main()
