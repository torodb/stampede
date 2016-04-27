CREATE OR REPLACE FUNCTION torodb.to_json_value(boolean)
RETURNS text AS $$
    SELECT CASE WHEN $1 IS NULL THEN 'null' WHEN $1 THEN 'true' ELSE 'false' END;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION torodb.to_json_value(numeric)
RETURNS text AS $$
    SELECT CASE WHEN $1 IS NULL THEN 'null' ELSE $1::text END;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION torodb.to_json_value(integer)
RETURNS text AS $$
    SELECT torodb.to_json_value($1::numeric);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION torodb.to_json_value(double precision)
RETURNS text AS $$
    SELECT torodb.to_json_value($1::numeric);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION torodb.to_json_value(text)
RETURNS text AS $$
    SELECT CASE WHEN $1 IS NULL THEN 'null' ELSE '"'||translate(regexp_replace($1,E'("|\\|/|'||chr(8)||'|'||chr(12)||'|'||chr(10)||'|'||chr(13)||'|'||chr(9)||')',E'\\\\\\1','g'),chr(8)||chr(12)||chr(10)||chr(13)||chr(9),'bfnrt')||'"' END;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION torodb.to_json_value(varchar)
RETURNS text AS $$
    SELECT torodb.to_json_value($1::text);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION torodb.to_json_value(bytea)
RETURNS text AS $$
    SELECT CASE WHEN $1 IS NULL THEN 'null' ELSE '"'||E'\\\\x'||encode($1,'hex')||'"' END;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION torodb.to_json_value(json)
RETURNS text AS $$
    SELECT CASE WHEN $1 IS NULL THEN 'null' ELSE $1 END;
$$ LANGUAGE SQL;
