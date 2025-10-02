-- 015 functions and macros.sql

CREATE OR REPLACE MACRO clean_authors(input_text) AS
    CASE
        WHEN input_text LIKE '.Wp-%' THEN ''
        WHEN input_text LIKE '%, .Wp-%' THEN SPLIT_PART(input_text, ', .Wp-', 1)
        ELSE input_text
    END;