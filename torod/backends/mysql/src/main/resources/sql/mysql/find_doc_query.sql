DROP PROCEDURE IF EXISTS torodb.find_doc_query;

DELIMITER $$

CREATE PROCEDURE torodb.find_doc_query(col_schema varchar(64), did integer, tables json, orderBy text, OUT query text)
READS SQL DATA
BEGIN
    DECLARE t integer;
    DECLARE pos integer DEFAULT 0;
    SET query = CONCAT('SELECT ', did, ', CAST(null AS UNSIGNED), root.sid, CAST(null AS json) FROM (SELECT * FROM `', col_schema, '`.root WHERE did = ', did, ') AS root');
    slice_loop: LOOP
        IF pos >= json_length(tables) THEN
            LEAVE slice_loop;
        END IF;
        SET t=json_extract(tables, CONCAT('$[', pos, ']'));
        SET @column_query = CONCAT('SELECT GROUP_CONCAT(CONCAT('''''''', COLUMN_NAME, '''''', subdoc.'', COLUMN_NAME))',
                ' FROM information_schema.columns',
                ' WHERE TABLE_SCHEMA = ''', col_schema, ''' AND TABLE_NAME = ''t_', t, ''' INTO @columns');
        PREPARE stmt FROM @column_query;
        EXECUTE stmt;
        SET query=CONCAT(query, ' UNION ALL SELECT ', did, ', ', t, ', subdoc.index, json_object(', @columns, ')',
                ' FROM (SELECT * FROM `', col_schema, '`.t_', t, ' WHERE did = ', did, ') as subdoc');
        SET pos=pos+1;
    END LOOP slice_loop;
    IF orderBy IS NOT NULL THEN
        SET query=CONCAT(query, ' ', orderBy);
    END IF;
END$$
