DROP FUNCTION IF EXISTS torodb.first_free_doc_id;

DELIMITER $$

CREATE FUNCTION torodb.first_free_doc_id(col_schema varchar(64))
RETURNS int
NOT DETERMINISTIC
MODIFIES SQL DATA
BEGIN
    RETURN (SELECT torodb.nextval(col_schema, 'root_seq'));
END$$