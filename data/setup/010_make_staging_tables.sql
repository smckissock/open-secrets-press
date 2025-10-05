DROP TABLE IF EXISTS stage_newspaper;


CREATE TABLE stage_newspaper (
    media_cloud_id  VARCHAR,
    import_date     TIMESTAMP,
    title           VARCHAR,
    text            TEXT,
    publish_date    TIMESTAMP,
    authors         VARCHAR,
    top_image       VARCHAR,
    summary         TEXT,
    success         BOOLEAN,
    error           VARCHAR
);


CREATE TABLE stage_sentence (
    media_cloud_id  VARCHAR,
    import_date     TIMESTAMP,
    sentence        VARCHAR
);

