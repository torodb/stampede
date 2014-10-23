CREATE OR REPLACE FUNCTION first_free_doc_id(collection varchar)
RETURNS integer AS $$
DECLARE
    seq_name varchar := collection_to_schema(collection) || '.root_seq';
BEGIN
    return nextval(seq_name);
END;
$$ LANGUAGE plpgsql;