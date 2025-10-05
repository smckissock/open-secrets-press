DROP TABLE IF EXISTS story;


CREATE SEQUENCE IF NOT EXISTS seq_story START 1;
CREATE OR REPLACE TABLE story (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('seq_story'),
    edit_time           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    syndicator_id       INTEGER   NOT NULL DEFAULT 1,
    media_outlet_id     INTEGER   NOT NULL DEFAULT 1,
    media_cloud_id      VARCHAR   NOT NULL UNIQUE,
    url                 VARCHAR   NOT NULL UNIQUE,
    publish_date        TIMESTAMP NOT NULL DEFAULT TIMESTAMP '2000-01-01 00:00:00',
    title               VARCHAR   NOT NULL,
    language            VARCHAR   NOT NULL DEFAULT 'en',
    authors             VARCHAR   NOT NULL DEFAULT '',
    image               VARCHAR   NOT NULL DEFAULT '',
    body                VARCHAR   NOT NULL DEFAULT '',
    sentence            VARCHAR   NOT NULL DEFAULT '',
    summary             VARCHAR   NOT NULL DEFAULT '',
    active              BOOLEAN   NOT NULL DEFAULT TRUE,
    FOREIGN KEY (syndicator_id)   REFERENCES syndicator(id),
    FOREIGN KEY (media_outlet_id) REFERENCES media_outlet(id),
);


INSERT INTO story (
    syndicator_id,
    media_outlet_id,
    media_cloud_id,
    publish_date,
    title,
    url,
    language,
    authors,
    image,
    body,
    summary,
    sentence,
    active
)
SELECT 
    1,                    -- syndicator_id (1 for now)
    media_outlet_id,      -- from the view (COALESCE ensures default of 1)
    media_cloud_id,       -- from stage_story.id
    publish_date,
    title,
    url,
    language,
    authors,              -- from stage_newspaper
    image,                -- from stage_newspaper.top_image
    body,                 -- from stage_newspaper.text
    summary,              -- from stage_newspaper
    sentence,
    1                     -- active 
FROM populate_story_view;