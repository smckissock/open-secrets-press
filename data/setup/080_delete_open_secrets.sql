DELETE FROM stage_story WHERE media_name = 'opensecrets.org';
DELETE FROM stage_newspaper WHERE top_image LIKE '%opensecrets.org%'
DELETE FROM story WHERE url LIKE '%www.opensecrets.org%' 

