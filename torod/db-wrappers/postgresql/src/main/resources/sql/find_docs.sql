CREATE OR REPLACE FUNCTION find_docs(collection varchar, dids int[], tables int[])
RETURNS SETOF find_doc_type AS $$
DECLARE
    col_schema varchar := collection_to_schema(collection);
    docid integer;
BEGIN

    FOR docid IN 
        EXECUTE 'SELECT did FROM ' || col_schema || '.root WHERE EXISTS (SELECT 1 FROM unnest($1) WHERE unnest = did)' USING dids
    LOOP
        RETURN QUERY select * from find_doc(collection, docid, tables);
    END LOOP;

END;
$$ LANGUAGE plpgsql;