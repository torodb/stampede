CREATE OR REPLACE FUNCTION torodb.reserve_doc_ids(col_schema varchar, increment integer)
RETURNS integer AS $$
DECLARE
    seq_name varchar := '"' || col_schema || '".root_seq';
    temp integer;
BEGIN
    select setval(seq_name, nextval(seq_name) + increment - 1) INTO temp;

    return temp;
END;
$$ LANGUAGE plpgsql;