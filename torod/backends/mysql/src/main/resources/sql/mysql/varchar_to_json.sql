DROP FUNCTION IF EXISTS torodb.varchar_to_json;

DELIMITER $$

CREATE FUNCTION torodb.varchar_to_json(json_string text)
RETURNS json
DETERMINISTIC
CONTAINS SQL
BEGIN
    RETURN (SELECT CAST(json_string AS json));
END$$