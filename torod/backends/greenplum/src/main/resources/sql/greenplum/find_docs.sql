CREATE OR REPLACE FUNCTION torodb.find_docs(col_schema varchar, dids int[], tables int[])
RETURNS SETOF torodb.find_doc_type AS $$
DECLARE
    docid integer;
    query_record record;
BEGIN

    FOR docid IN 
        EXECUTE 'SELECT did FROM "' || col_schema || '".root WHERE did IN ('||array_to_string(dids,',')||')'
    LOOP
        FOR query_record IN SELECT * FROM torodb.find_doc(col_schema, docid, tables) LOOP
            RETURN NEXT query_record;
        END LOOP;
    END LOOP;

END;
$$ LANGUAGE plpgsql;