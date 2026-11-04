-- Geo queries: PostGIS geometry derived from the entity GeoJSON location.
-- Skipped when PostGIS is not available (geo query parameters then answer 501).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'postgis') THEN
    CREATE EXTENSION IF NOT EXISTS postgis;
    EXECUTE $sql$
      ALTER TABLE entities ADD COLUMN geom geometry(Geometry, 4326)
        GENERATED ALWAYS AS (
          CASE WHEN location IS NULL THEN NULL
               ELSE ST_SetSRID(ST_GeomFromGeoJSON(location::text), 4326) END
        ) STORED
    $sql$;
    EXECUTE 'CREATE INDEX entities_geom_idx ON entities USING gist (geom)';
  END IF;
END
$$;
