-- 060 populate story sentence

UPDATE story
SET sentence = ss.sentence
FROM stage_sentence ss
WHERE story.media_cloud_id = ss.media_cloud_id
AND (story.sentence IS NULL OR story.sentence = '');