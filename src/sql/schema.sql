-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create main schema
CREATE SCHEMA IF NOT EXISTS data;

-- Timestamp update function
CREATE OR REPLACE FUNCTION data.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- FloodNet deployments table with geometry
CREATE TABLE IF NOT EXISTS data.floodnet_deployments (
    deployment_id TEXT PRIMARY KEY,
    sensor_name TEXT,
    sensor_status TEXT,
    date_deployed TIMESTAMP WITH TIME ZONE,
    location GEOMETRY(POINT, 4326) NOT NULL,  -- Using ST_Point() for insertions
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create unique index for deployment_id
CREATE UNIQUE INDEX IF NOT EXISTS deployments_deployment_id_idx 
    ON data.floodnet_deployments(deployment_id);

-- FloodNet readings table
CREATE TABLE IF NOT EXISTS data.floodnet_readings (
    id SERIAL PRIMARY KEY,
    deployment_id TEXT REFERENCES data.floodnet_deployments(deployment_id),
    time TIMESTAMP WITH TIME ZONE,
    depth_proc_mm DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
-- Add UNIQUE index for readings
CREATE UNIQUE INDEX IF NOT EXISTS readings_deployment_time_unique_idx 
    ON data.floodnet_readings(deployment_id, time);

-- Regular indexes for performance
CREATE INDEX IF NOT EXISTS readings_time_idx 
    ON data.floodnet_readings(time);
CREATE INDEX IF NOT EXISTS deployments_location_idx 
    ON data.floodnet_deployments USING GIST(location);

-- Trigger for updated_at
CREATE OR REPLACE TRIGGER update_deployments_updated_at
    BEFORE UPDATE ON data.floodnet_deployments
    FOR EACH ROW
    EXECUTE FUNCTION data.update_updated_at_column();

-- Permissions
GRANT USAGE ON SCHEMA data TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data TO airflow;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA data TO airflow;
