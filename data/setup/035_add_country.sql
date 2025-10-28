CREATE OR REPLACE TABLE country (
    id INTEGER DEFAULT (nextval ('seq_state')) PRIMARY KEY,
    edit_time TIMESTAMP DEFAULT (CURRENT_TIMESTAMP) NOT NULL,
    "name" VARCHAR NOT NULL UNIQUE
  );

INSERT INTO country (name) VALUES('Unspecified');

INSERT INTO country (name) VALUES ('United States');
INSERT INTO country (name) VALUES ('United Kingdom');
INSERT INTO country (name) VALUES ('Canada');
INSERT INTO country (name) VALUES ('Singapore');
INSERT INTO country (name) VALUES ('Japan');
INSERT INTO country (name) VALUES ('India');
INSERT INTO country (name) VALUES ('Mexico');
INSERT INTO country (name) VALUES ('Ireland');
INSERT INTO country (name) VALUES ('China');
INSERT INTO country (name) VALUES ('Australia');
INSERT INTO country (name) VALUES ('South Korea');
INSERT INTO country (name) VALUES ('Israel');
INSERT INTO country (name) VALUES ('Pakistan');
INSERT INTO country (name) VALUES ('France');
INSERT INTO country (name) VALUES ('Russia');
INSERT INTO country (name) VALUES ('Hong Kong');
INSERT INTO country (name) VALUES ('Qatar');
INSERT INTO country (name) VALUES ('Ukraine');
INSERT INTO country (name) VALUES ('Spain');