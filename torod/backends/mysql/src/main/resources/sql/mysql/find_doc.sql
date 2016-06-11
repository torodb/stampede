DROP PROCEDURE IF EXISTS torodb.find_doc;

DELIMITER $$

CREATE PROCEDURE torodb.find_doc(col_schema varchar(64), did integer, tables json, orderBy text)
READS SQL DATA
BEGIN
    CALL torodb.find_doc_query(col_schema, did, tables, orderBy, @query);
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
