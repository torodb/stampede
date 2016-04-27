CREATE OR REPLACE FUNCTION torodb.find_doc(col_schema varchar, did integer, tables integer[])
RETURNS SETOF torodb.find_doc_type AS $$
DECLARE
    t integer;
    query text;
    query_record record;
    attname text;
    i integer := 0;
    n integer := array_upper(tables, 1);
    indexFilter integer[];
    indexFilterAsText varchar;
BEGIN
	FOR query_record IN EXECUTE 
		torodb.find_doc_query(col_schema, did, tables)
	LOOP
	    RETURN NEXT query_record;
	END LOOP;
END;
$$ LANGUAGE plpgsql;