-- 015 functions and macros.sql

CREATE OR REPLACE MACRO clean_authors(input_text) AS
    CASE
        WHEN input_text LIKE '.Wp-%' THEN ''
        WHEN input_text LIKE '%, .Wp-%' THEN SPLIT_PART(input_text, ', .Wp-', 1)
        ELSE input_text
    END;



 CREATE OR REPLACE MACRO extract_domain(url) AS (
    WITH processed AS (
        SELECT 
            -- Remove protocol and convert to lowercase
            LOWER(REPLACE(REPLACE(url, 'https://', ''), 'http://', '')) AS step1
    ),
    domain_only AS (
        SELECT 
            -- Extract before first slash
            CASE 
                WHEN POSITION('/' IN step1) > 0 
                THEN SUBSTRING(step1, 1, POSITION('/' IN step1) - 1)
                ELSE step1
            END AS step2
        FROM processed
    ),
    without_www AS (
        SELECT 
            -- Remove www. prefix
            CASE 
                WHEN LEFT(step2, 4) = 'www.' 
                THEN SUBSTRING(step2, 5)
                ELSE step2
            END AS domain
        FROM domain_only
    ),
    final AS (
        SELECT 
            domain,
            string_split(domain, '.') AS parts,
            array_length(string_split(domain, '.')) AS total_parts
        FROM without_www
    )
    SELECT 
        CASE
            -- Special handling for substack.com
            WHEN domain LIKE '%.substack.com' THEN domain
            -- Handle common two-part TLDs (co.uk, com.au, etc.)
            WHEN total_parts >= 3 AND parts[total_parts - 1] IN ('co', 'com', 'org', 'net', 'gov', 'edu', 'ac') THEN
                parts[total_parts - 2] || '.' || parts[total_parts - 1] || '.' || parts[total_parts]
            -- Extract main domain + TLD for multi-part domains
            WHEN total_parts > 1 THEN 
                parts[total_parts - 1] || '.' || parts[total_parts]
            ELSE domain
        END
    FROM final
);   