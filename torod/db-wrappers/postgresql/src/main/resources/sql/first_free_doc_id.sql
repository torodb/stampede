CREATE OR REPLACE FUNCTION torodb.first_free_doc_id(col_schema varchar)
RETURNS integer AS $$
DECLARE
    seq_name varchar := '"' || col_schema || '".root_seq';
BEGIN
    return nextval(seq_name);
END;
$$ LANGUAGE plpgsql;