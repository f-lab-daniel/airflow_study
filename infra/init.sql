-- postgres 컨테이너가 처음 뜰 때 한 번 실행됩니다.

-- 1) Airflow 메타데이터 DB
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
CREATE DATABASE airflow_db OWNER airflow_user ENCODING 'UTF8';
ALTER USER airflow_user SET search_path = public;

-- 2) DAG 실습 DB
CREATE USER study_user WITH PASSWORD 'study_pass';
CREATE DATABASE study_db OWNER study_user ENCODING 'UTF8';

\connect study_db study_user

-- 2_postgres_loader.py
CREATE TABLE public.sample_table (
    id    serial PRIMARY KEY,
    key   VARCHAR(50) NOT NULL,
    value VARCHAR(50) NOT NULL
);

-- 3_http_dag.py
CREATE TABLE public.starwars_character (
    name   TEXT NOT NULL,
    height TEXT NOT NULL,
    mass   TEXT NOT NULL
);
