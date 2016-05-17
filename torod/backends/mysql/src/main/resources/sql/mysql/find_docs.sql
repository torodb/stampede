DROP PROCEDURE IF EXISTS torodb.find_docs;

DELIMITER $$

CREATE PROCEDURE torodb.find_docs(col_schema varchar(64), dids json, tables json, orderBy text)
READS SQL DATA
BEGIN
    DECLARE did integer;
    DECLARE pos integer DEFAULT 0;
    DECLARE query text DEFAULT NULL;
    slice_loop: LOOP
        IF pos >= json_length(dids) THEN
            LEAVE slice_loop;
        END IF;
        SET did=json_extract(dids, CONCAT('$[', pos, ']'));
        CALL torodb.find_doc_query(col_schema, did, tables, null, @did_query);
        IF query IS NULL THEN
            SET query=@did_query;
        ELSE
            SET query=CONCAT(query, ' UNION ALL ', @did_query);
        END IF;
        SET pos=pos+1;
    END LOOP slice_loop;
    IF orderBy IS NOT NULL THEN
        SET query=CONCAT(query, ' ', orderBy);
    END IF;
    SET @query=query;
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$