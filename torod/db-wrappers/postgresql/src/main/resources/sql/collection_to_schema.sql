CREATE OR REPLACE FUNCTION collection_to_schema(collection varchar)
RETURNS varchar AS $$
BEGIN
    return 'col_' || collection;
END;
$$ LANGUAGE plpgsql;