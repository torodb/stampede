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
		'SELECT '||did||', null::integer, root.sid, null::text'||
		' FROM (SELECT * FROM "' || col_schema || '".root WHERE did = '||did||') AS root'
	LOOP
	    RETURN NEXT query_record;
	END LOOP;
    FOR t IN SELECT name FROM unnest(tables) as name
    LOOP
        query := 'SELECT '||did||', ' || t || ', index, ''{';
	    FOR attname IN EXECUTE 'select attname from pg_namespace as n left join pg_class as c on (n.oid = c.relnamespace) left join pg_attribute as a'||
	           ' on (c.oid = a.attrelid and attnum > 0) where nspname = '''||col_schema||''' AND relname = ''t_'||t||'''' LOOP
           RAISE NOTICE 'subdoc query: %', query;
           RAISE NOTICE 'adding column: %', attname;
	       query := query || torodb.to_json_value(attname) ||
	           ':''||torodb.to_json_value("'||attname||'")||'',';
	    END LOOP;
        RAISE NOTICE 'subdoc query: %', query;
	    query := substring(query,1,length(query)-1) || '}''' ||
	    	' FROM (SELECT * FROM "' || col_schema || '"."t_' || t || '" WHERE did = '||did||') as subdoc';
	    RAISE NOTICE 'subdoc query: %', query;
		FOR query_record IN EXECUTE query LOOP
		    RETURN NEXT query_record;
		END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;