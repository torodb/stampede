DROP PROCEDURE IF EXISTS torodb.create_sequence;

DELIMITER $$

CREATE PROCEDURE torodb.create_sequence(seq_schema varchar(64), seq_name varchar(64))
MODIFIES SQL DATA
BEGIN
    DELETE FROM torodb.sequence 
        WHERE sequence_schema = seq_schema AND sequence_name = seq_name;
    
    INSERT INTO torodb.sequence (sequence_schema, sequence_name)
        VALUES (seq_schema, seq_name);
END$$
