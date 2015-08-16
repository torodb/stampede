CREATE OR REPLACE FUNCTION torodb.varchar_to_jsonb(varchar) RETURNS jsonb AS $$
SELECT jsonb_in($1::cstring); 
$$ LANGUAGE SQL IMMUTABLE;