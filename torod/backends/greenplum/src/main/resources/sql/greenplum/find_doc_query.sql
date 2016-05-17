CREATE OR REPLACE FUNCTION torodb.find_doc_query(col_schema varchar, did integer, tables integer[])
RETURNS text AS $$
DECLARE
    query text;
    query_record record;
    attname text;
    t integer;
    n integer := array_upper(tables, 1);
BEGIN
	query := 'SELECT '||did||', null::integer, root.sid, null::text'||
        ' FROM (SELECT * FROM "' || col_schema || '".root WHERE did = '||did||') AS root';
    FOR t IN SELECT name FROM unnest(tables) as name
    LOOP
        query := query || ' UNION ALL SELECT '||did||', ' || t || ', index, ''{';
	    FOR attname IN EXECUTE 'select attname from pg_namespace as n left join pg_class as c on (n.oid = c.relnamespace) left join pg_attribute as a'||
	           ' on (c.oid = a.attrelid and attnum > 0) where nspname = '''||col_schema||''' AND relname = ''t_'||t||'''' LOOP
	       query := query || torodb.to_json_value(attname) ||
	           ':''||torodb.to_json_value("'||attname||'")||'',';
	    END LOOP;
	    query := substring(query,1,length(query)-1) || '}''' ||
	    	' FROM (SELECT * FROM "' || col_schema || '"."t_' || t || '" WHERE did = '||did||') as subdoc';
    END LOOP;
    
    RETURN query;
END;
$$ LANGUAGE plpgsql;