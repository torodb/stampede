DROP FUNCTION IF EXISTS torodb.reserve_doc_ids;

DELIMITER $$

CREATE FUNCTION torodb.reserve_doc_ids(col_schema varchar(64), increment integer)
RETURNS integer
NOT DETERMINISTIC
MODIFIES SQL DATA
BEGIN
    DECLARE seq_name varchar(64) DEFAULT 'root_seq';
    
    UPDATE torodb.sequence
        SET sequence_cur_value = IF (
            (sequence_cur_value + increment) > sequence_max_value,
                IF (
                    sequence_cycle = TRUE,
                    sequence_min_value,
                    NULL
                ),
                sequence_cur_value + increment
            )
        WHERE sequence_schema = col_schema
        AND sequence_name = seq_name;
    
    RETURN (SELECT sequence_cur_value
        FROM torodb.sequence
        WHERE sequence_schema = col_schema
        AND sequence_name = seq_name);
END$$
