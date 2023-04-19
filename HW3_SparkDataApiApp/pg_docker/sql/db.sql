SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_with_oids = false;

DROP DATABASE IF EXISTS pg_test_db;
CREATE DATABASE pg_test_db;

CREATE TABLE IF NOT EXISTS distance_distribution (
    borough         VARCHAR(100)    NOT NULL,
    trips_count     INT             NOT NULL,
    mean_distance   FLOAT           NOT NULL,
    std_distance    FLOAT           NOT NULL,
    min_distance    FLOAT           NOT NULL,
    max_distance    FLOAT           NOT NULL,
    PRIMARY KEY (borough)
);
