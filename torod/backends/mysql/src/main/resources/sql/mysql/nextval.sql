DROP FUNCTION IF EXISTS torodb.nextval;

DELIMITER $$

CREATE FUNCTION torodb.nextval(seq_schema varchar(64), seq_name varchar(64))
RETURNS bigint
NOT DETERMINISTIC
MODIFIES SQL DATA
BEGIN
    UPDATE torodb.sequence
        SET sequence_cur_value = IF (
            (sequence_cur_value + sequence_increment) > sequence_max_value,
                IF (
                    sequence_cycle = TRUE,
                    sequence_min_value,
                    NULL
                ),
                sequence_cur_value + sequence_increment
            )
        WHERE sequence_schema = seq_schema
        AND sequence_name = seq_name;
    
    RETURN (SELECT sequence_cur_value
        FROM torodb.sequence
        WHERE sequence_schema = seq_schema
        AND sequence_name = seq_name);
END$$
