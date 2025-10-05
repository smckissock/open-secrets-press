-- 070 views.sql

CREATE OR REPLACE VIEW populate_story_view
AS
SELECT 
  1,
  COALESCE(m.id, 1) media_outlet_id,
  s.id media_cloud_id,
  s.publish_date,
  s.title,
  s.url,
  s.language,
  clean_authors(n.authors) authors,
  n.top_image image,
  n.text body,
  n.summary,
  COALESCE(sen.sentence, '') sentence,
  1
FROM stage_story s
JOIN stage_newspaper n ON n.media_cloud_id = s.id
LEFT JOIN stage_sentence sen ON s.id = sen.media_cloud_id
LEFT JOIN media_outlet m ON m.domain_name = s.media_url;


CREATE OR REPLACE VIEW media_outlet_view AS
SELECT 
    m.id,
    m.edit_time,
    m.name,
    m.domain_name,
    mot.name AS media_outlet_type,
    mot.media_cloud_code,
    s.name AS state,
    s.code AS state_code,
    s.is_state,
    br.name AS bias_rating,
    m.note,
    m.active
FROM media_outlet m
JOIN media_outlet_type mot ON m.media_outlet_type_id = mot.id
JOIN state s ON m.state_id = s.id
JOIN bias_rating br ON m.bias_rating_id = br.id;


CREATE OR REPLACE VIEW story_web_view AS
SELECT 
    s.id,
    s.publish_date,
    s.title,
    s.url,
    s.authors,
    s.image,
    s.summary,
    s.language,
    m.name media_outlet,
    m.media_outlet_type,
    COALESCE(NULLIF(m.state, 'Unspecified'), 'National') AS state,
    m.state_code,
    m.bias_rating,
    s.sentence,
    syn.name AS syndicator
FROM story s
JOIN media_outlet_view m ON s.media_outlet_id = m.id
JOIN syndicator syn ON s.syndicator_id = syn.id
WHERE s.active = TRUE 
  AND m.active = TRUE
ORDER BY s.publish_date DESC;