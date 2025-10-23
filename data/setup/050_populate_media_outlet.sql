-- 030 populate media outlet.sql


-- Doesn't seem to be a good way to ensure seq_media_outlet starts at 1, so set it on insert
INSERT INTO media_outlet (
    id,
    name,
    domain_name,
    media_outlet_type_id,
    state_id,
    bias_rating_id,
    note,
    active
)
SELECT 1, 'Unspecified', 'None', 1, 1, 1, '', 1;


INSERT INTO media_outlet (
    name,
    domain_name,
    media_outlet_type_id,
    state_id,
    bias_rating_id,
    note,
    active
)
SELECT
    s.name,
    s.domain_name,
    mot.id,
    st.id,
    br.id,
    '',
    s.active
FROM stage_media_outlet s
JOIN media_outlet_type mot ON mot.name = s.media_outlet_type
JOIN state st              ON st.name = s.state
JOIN bias_rating br        ON br.name = s.bias
WHERE s.name <> 'Unspecified';


-- ALSO THIS!
 INSERT INTO media_outlet (
   media_outlet_type_id,
   state_id,
   bias_rating_id,
   name,
   domain_name,
   note,
   active
 ) VALUES (
   8, -- Advocacy group
   1, 
   4, -- center
   'OpenSecrets',
   'opensecrets.org',
   '',
   true
 )