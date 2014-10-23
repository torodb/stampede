CREATE OR REPLACE FUNCTION reserve_doc_ids(collection varchar, increment integer)
RETURNS integer AS $$
DECLARE
    seq_name varchar := collection_to_schema(collection) || '.root_seq';
    temp integer;
BEGIN
    select setval(seq_name, nextval(seq_name) + increment - 1) INTO temp;

    return temp;
END;
$$ LANGUAGE plpgsql;