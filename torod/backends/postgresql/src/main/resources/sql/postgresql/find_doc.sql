CREATE OR REPLACE FUNCTION torodb.find_doc(col_schema varchar, did integer, tables integer[])
RETURNS SETOF torodb.find_doc_type AS $$
DECLARE
    t integer;
    i integer := 0;
    n integer := array_length(tables, 1);
    indexFilter integer[];
    indexFilterAsText varchar;
BEGIN

    RETURN QUERY EXECUTE 
        'SELECT $1, null::integer, root.sid, null::text FROM (SELECT * FROM "' || col_schema || '".root WHERE did = $1) AS root'
        USING did;
    FOREACH t IN ARRAY tables
    LOOP
        RETURN QUERY EXECUTE 
            'SELECT $1, ' || t || ', subdoc.index, to_json(subdoc)::text
                FROM (SELECT * FROM "' || col_schema || '"."t_' || t || '" WHERE did = $1) as subdoc'
            USING did;
    END LOOP;
END;
$$ LANGUAGE plpgsql;