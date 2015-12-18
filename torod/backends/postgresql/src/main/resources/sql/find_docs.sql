CREATE OR REPLACE FUNCTION torodb.find_docs(col_schema varchar, dids int[], tables int[])
RETURNS SETOF torodb.find_doc_type AS $$
DECLARE
    docid integer;
BEGIN

    FOR docid IN 
        EXECUTE 'SELECT did FROM "' || col_schema || '".root WHERE EXISTS (SELECT 1 FROM unnest($1) WHERE unnest = did)' USING dids
    LOOP
        RETURN QUERY select * from torodb.find_doc(col_schema, docid, tables);
    END LOOP;

END;
$$ LANGUAGE plpgsql;