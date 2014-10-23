CREATE OR REPLACE FUNCTION create_collection_schema(collection varchar)
RETURNS void AS $$
DECLARE
    col_schema varchar := collection_to_schema(collection);
BEGIN
    EXECUTE 'CREATE SCHEMA '   || col_schema;
    EXECUTE 'CREATE SEQUENCE ' || col_schema || '.root_seq MINVALUE 0 START 0';

    EXECUTE 'CREATE TABLE '    || col_schema || '.root (
        did int PRIMARY KEY DEFAULT nextval(''' || col_schema || '.' || 'root_seq''),
        sid int NOT NULL)';

    EXECUTE 'CREATE TABLE '    || col_schema || '.structures (
        sid int PRIMARY KEY,
        _structure jsonb NOT NULL)';

END;
$$ LANGUAGE plpgsql;