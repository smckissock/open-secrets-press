
-- Could be on the website but aren't (need media_outlet_id)
SELECT extract_domain(url), COUNT(*) 
FROM story_web_view 
WHERE media_outlet = 'Unspecified'
GROUP BY ALL
ORDER BY COUNT(*) DESC


-- Counts of stories with and without media outlets
 SELECT 'stage_story', COUNT(*) FROM stage_story GROUP BY ALL
 UNION ALL
 SELECT 'story_web_view', COUNT(*) FROM story_web_view GROUP BY ALL
 UNION ALL 
 SELECT 'story_web_view - media specified', COUNT(*) FROM story_web_view WHERE media_outlet <> 'Unspecified' GROUP BY ALL
 UNION ALL
 SELECT 'story_web_view - media unspecified', COUNT(*) FROM story_web_view WHERE media_outlet = 'Unspecified' GROUP BY ALL
 



INSERT INTO media_outlet (media_outlet_type_id, state_id, bias_rating_id, domain_name, name, note) VALUES (
  (SELECT id from media_outlet_type WHERE name = 'Web Publication'),
  (SELECT id from state WHERE name = 'Unspecified'),
  (SELECT id from bias_rating WHERE name = 'Center Left'),
  'huffingtonpost.com',
  'Huffington Post', ''
); 

INSERT INTO media_outlet (media_outlet_type_id, state_id, bias_rating_id, domain_name, name, note) VALUES (
  (SELECT id from media_outlet_type WHERE name = 'Web Publication'),
  (SELECT id from state WHERE name = 'Unspecified'),
  (SELECT id from bias_rating WHERE name = 'Center Right'),
  'washingtonexaminer.com',
  'Washington Examiner', ''
);

INSERT INTO media_outlet (media_outlet_type_id, state_id, bias_rating_id, domain_name, name, note) VALUES (
  (SELECT id from media_outlet_type WHERE name = 'Web Publication'),
  (SELECT id from state WHERE name = 'Unspecified'),
  (SELECT id from bias_rating WHERE name = 'Center'),
  'indiatimes.com',
  'India Times', ''
);

INSERT INTO media_outlet (media_outlet_type_id, state_id, bias_rating_id, domain_name, name, note) VALUES (
  (SELECT id from media_outlet_type WHERE name = 'Web Publication'),
  (SELECT id from state WHERE name = 'Unspecified'),
  (SELECT id from bias_rating WHERE name = 'Center Left'),
  'politifact.com',
  'PolitiFact', ''
);

INSERT INTO media_outlet (media_outlet_type_id, state_id, bias_rating_id, domain_name, name, note) VALUES (
  (SELECT id from media_outlet_type WHERE name = 'Web Publication'),
  (SELECT id from state WHERE name = 'Unspecified'),
  (SELECT id from bias_rating WHERE name = 'Center Left'),
  'snopes.com',
  'Snopes', ''
);

INSERT INTO media_outlet (media_outlet_type_id, state_id, bias_rating_id, domain_name, name, note) VALUES (
  (SELECT id from media_outlet_type WHERE name = 'Web Publication'),
  (SELECT id from state WHERE name = 'Unspecified'),
  (SELECT id from bias_rating WHERE name = 'Left'),
  'jacobinmag.com',
  'Jacobin', ''
);

INSERT INTO media_outlet (media_outlet_type_id, state_id, bias_rating_id, domain_name, name, note) VALUES (
  (SELECT id from media_outlet_type WHERE name = 'Web Publication'),
  (SELECT id from state WHERE name = 'Unspecified'),
  (SELECT id from bias_rating WHERE name = 'Left'),
  'thinkprogress.org',
  'Think Progress', ''
) ;