CREATE DATABASE PI_FLOW;

CREATE SCHEMA PI_FLOW.METADATA;

USE DATABASE PI_FLOW;
USE SCHEMA METADATA;


SELECT CURRENT_ACCOUNT(), CURRENT_REGION();

show warehouses;


--
CREATE TABLE DAG (
    dag_id STRING PRIMARY KEY,
    fileloc STRING,
    schedule_interval STRING,
    is_paused BOOLEAN,
    owners STRING,
    description STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    tags STRING
);

CREATE OR REPLACE TABLE PI_FLOW.METADATA.DAG_FILE (
    FILE_PATH VARCHAR NOT NULL,
    DAG_ID VARCHAR NOT NULL,
    FILE_HASH VARCHAR NOT NULL,
    LAST_PARSED_AT TIMESTAMP_NTZ,
    PRIMARY KEY (FILE_PATH),
    CONSTRAINT FK_DAG_FILE_DAG
      FOREIGN KEY (DAG_ID)
      REFERENCES PI_FLOW.METADATA.DAG(DAG_ID)
);

CREATE TABLE DAG_RUN (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    dag_id STRING,
    run_id STRING,
    state STRING,
    execution_date TIMESTAMP,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    conf STRING,
    run_type STRING,
    
    -- logical reference
    CONSTRAINT fk_dagrun_dag
        FOREIGN KEY (dag_id) REFERENCES DAG(dag_id)
);

CREATE TABLE TASK_INSTANCE (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    dag_id STRING,
    task_id STRING,
    run_id STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    state STRING,
    try_number INTEGER,
    operator STRING,
    duration FLOAT,
    hostname STRING,
    log_filepath STRING,

    -- references DAG + DAG_RUN
    CONSTRAINT fk_ti_dag
        FOREIGN KEY (dag_id) REFERENCES DAG(dag_id)
);

CREATE TABLE JOB (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    dag_id STRING,
    state STRING,
    job_type STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    hostname STRING,

    CONSTRAINT fk_job_dag
        FOREIGN KEY (dag_id) REFERENCES DAG(dag_id)
);

CREATE TABLE LOG (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    dag_id STRING,
    task_id STRING,
    run_id STRING,
    try_number INTEGER,
    event STRING,
    when TIMESTAMP,
    message STRING
);

CREATE TABLE VARIABLE (
    key STRING PRIMARY KEY,
    value STRING
);

CREATE TABLE CONNECTION (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    conn_id STRING UNIQUE,
    conn_type STRING,
    host STRING,
    schema STRING,
    login STRING,
    password STRING,
    port INTEGER,
    extra STRING
);







CREATE OR REPLACE TABLE PI_FLOW.METADATA.TASK (
    ID NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    DAG_ID VARCHAR(16777216) NOT NULL,
    TASK_ID VARCHAR(16777216) NOT NULL,
    OPERATOR VARCHAR(16777216),
    OWNER VARCHAR(16777216),
    DESCRIPTION VARCHAR(16777216),
    RETRIES NUMBER(38,0) DEFAULT 0,
    RETRY_DELAY_SECONDS NUMBER(38,0),
    PRIMARY KEY (ID),
    CONSTRAINT UK_TASK UNIQUE (DAG_ID, TASK_ID),
    CONSTRAINT FK_TASK_DAG FOREIGN KEY (DAG_ID)
        REFERENCES PI_FLOW.METADATA.DAG(DAG_ID)
);

ALTER TABLE PI_FLOW.METADATA.TASK
ADD COLUMN PARAMS VARIANT;


CREATE OR REPLACE TABLE PI_FLOW.METADATA.TASK_DEPENDENCY (
    ID NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    DAG_ID VARCHAR(16777216) NOT NULL,
    UPSTREAM_TASK_ID VARCHAR(16777216) NOT NULL,
    DOWNSTREAM_TASK_ID VARCHAR(16777216) NOT NULL,
    PRIMARY KEY (ID),
    CONSTRAINT UK_TASK_DEP UNIQUE (DAG_ID, UPSTREAM_TASK_ID, DOWNSTREAM_TASK_ID),
    CONSTRAINT FK_DEP_DAG FOREIGN KEY (DAG_ID)
        REFERENCES PI_FLOW.METADATA.DAG(DAG_ID)
);







-- Inserting Dummy data 

INSERT INTO DAG (dag_id, fileloc, schedule_interval, is_paused, owners, description, start_date, end_date, tags)
VALUES
('dag_1', '/dags/dag_1.py', '0 12 * * *', FALSE, 'alice', 'Daily data pipeline', '2025-01-01 00:00:00', NULL, '["daily","etl"]'),
('dag_2', '/dags/dag_2.py', '0 0 * * 0', FALSE, 'bob', 'Weekly summary', '2025-01-01 00:00:00', NULL, '["weekly","report"]'),
('dag_3', '/dags/dag_3.py', '@hourly', TRUE, 'carol', 'Hourly processing', '2025-01-01 00:00:00', NULL, '["hourly","etl"]'),
('dag_4', '/dags/dag_4.py', '0 6 * * *', FALSE, 'dave', 'Morning pipeline', '2025-01-01 00:00:00', NULL, '["daily","morning"]'),
('dag_5', '/dags/dag_5.py', '0 18 * * *', TRUE, 'eve', 'Evening aggregation', '2025-01-01 00:00:00', NULL, '["daily","evening"]'),
('dag_6', '/dags/dag_6.py', '@weekly', FALSE, 'frank', 'Weekly cleanup', '2025-01-01 00:00:00', NULL, '["weekly","cleanup"]'),
('dag_7', '/dags/dag_7.py', '0 3 * * *', FALSE, 'grace', 'Nightly backup', '2025-01-01 00:00:00', NULL, '["nightly","backup"]'),
('dag_8', '/dags/dag_8.py', '@daily', TRUE, 'heidi', 'Daily ingest', '2025-01-01 00:00:00', NULL, '["daily","ingest"]'),
('dag_9', '/dags/dag_9.py', '30 9 * * 1-5', FALSE, 'ivan', 'Weekday processing', '2025-01-01 00:00:00', NULL, '["weekday","etl"]'),
('dag_10', '/dags/dag_10.py', '@monthly', FALSE, 'judy', 'Monthly reports', '2025-01-01 00:00:00', NULL, '["monthly","report"]');


INSERT INTO DAG_RUN (dag_id, run_id, state, execution_date, start_date, end_date, conf, run_type)
VALUES
('dag_1','manual__2025-11-01','success','2025-11-01 12:00:00','2025-11-01 12:01:00','2025-11-01 12:05:00','{}','manual'),
('dag_1','scheduled__2025-11-02','running','2025-11-02 12:00:00','2025-11-02 12:00:10',NULL,'{}','scheduled'),
('dag_2','manual__2025-11-03','failed','2025-11-03 00:00:00','2025-11-03 00:01:00','2025-11-03 00:10:00','{}','manual'),
('dag_2','scheduled__2025-11-10','success','2025-11-10 00:00:00','2025-11-10 00:00:05','2025-11-10 00:08:00','{}','scheduled'),
('dag_3','manual__2025-11-01_01','success','2025-11-01 01:00:00','2025-11-01 01:00:01','2025-11-01 01:01:00','{}','manual'),
('dag_3','scheduled__2025-11-01_02','success','2025-11-01 02:00:00','2025-11-01 02:00:05','2025-11-01 02:01:00','{}','scheduled'),
('dag_4','manual__2025-11-01','running','2025-11-01 06:00:00','2025-11-01 06:01:00',NULL,'{}','manual'),
('dag_4','scheduled__2025-11-02','success','2025-11-02 06:00:00','2025-11-02 06:00:02','2025-11-02 06:03:00','{}','scheduled'),
('dag_5','manual__2025-11-01','success','2025-11-01 18:00:00','2025-11-01 18:00:05','2025-11-01 18:10:00','{}','manual'),
('dag_5','scheduled__2025-11-02','running','2025-11-02 18:00:00','2025-11-02 18:00:01',NULL,'{}','scheduled');


INSERT INTO TASK_INSTANCE (dag_id, task_id, run_id, start_date, end_date, state, try_number, operator, duration, hostname, log_filepath)
VALUES
-- DAG 1
('dag_1','task_a','manual__2025-11-01','2025-11-01 12:01:00','2025-11-01 12:02:00','success',1,'PythonOperator',60,'host1','/logs/dag_1/task_a.log'),
('dag_1','task_b','manual__2025-11-01','2025-11-01 12:02:05','2025-11-01 12:03:00','success',1,'PythonOperator',55,'host1','/logs/dag_1/task_b.log'),
('dag_1','task_c','manual__2025-11-01','2025-11-01 12:03:10','2025-11-01 12:05:00','success',1,'PythonOperator',110,'host1','/logs/dag_1/task_c.log'),
('dag_1','task_a','scheduled__2025-11-02','2025-11-02 12:00:10',NULL,'running',1,'PythonOperator',0,'host1','/logs/dag_1/task_a.log'),
('dag_1','task_b','scheduled__2025-11-02','2025-11-02 12:00:15',NULL,'queued',1,'PythonOperator',0,'host1','/logs/dag_1/task_b.log'),
('dag_1','task_c','scheduled__2025-11-02','2025-11-02 12:00:20',NULL,'queued',1,'PythonOperator',0,'host1','/logs/dag_1/task_c.log'),

-- DAG 2
('dag_2','task_a','manual__2025-11-03','2025-11-03 00:01:00','2025-11-03 00:05:00','failed',1,'PythonOperator',240,'host2','/logs/dag_2/task_a.log'),
('dag_2','task_b','manual__2025-11-03','2025-11-03 00:05:05','2025-11-03 00:08:00','skipped',1,'PythonOperator',175,'host2','/logs/dag_2/task_b.log'),
('dag_2','task_c','manual__2025-11-03','2025-11-03 00:08:10','2025-11-03 00:10:00','skipped',1,'PythonOperator',110,'host2','/logs/dag_2/task_c.log'),
('dag_2','task_a','scheduled__2025-11-10','2025-11-10 00:00:05','2025-11-10 00:08:00','success',1,'PythonOperator',475,'host2','/logs/dag_2/task_a.log'),
('dag_2','task_b','scheduled__2025-11-10','2025-11-10 00:00:10','2025-11-10 00:08:00','success',1,'PythonOperator',470,'host2','/logs/dag_2/task_b.log'),
('dag_2','task_c','scheduled__2025-11-10','2025-11-10 00:00:15','2025-11-10 00:08:00','success',1,'PythonOperator',465,'host2','/logs/dag_2/task_c.log'),

-- DAG 3
('dag_3','task_a','manual__2025-11-01_01','2025-11-01 01:00:00','2025-11-01 01:01:00','success',1,'PythonOperator',60,'host3','/logs/dag_3/task_a.log'),
('dag_3','task_b','manual__2025-11-01_01','2025-11-01 01:01:05','2025-11-01 01:02:00','success',1,'PythonOperator',55,'host3','/logs/dag_3/task_b.log'),
('dag_3','task_c','manual__2025-11-01_01','2025-11-01 01:02:10','2025-11-01 01:03:00','success',1,'PythonOperator',50,'host3','/logs/dag_3/task_c.log'),
('dag_3','task_a','scheduled__2025-11-01_02','2025-11-01 02:00:05',NULL,'running',1,'PythonOperator',0,'host3','/logs/dag_3/task_a.log'),
('dag_3','task_b','scheduled__2025-11-01_02','2025-11-01 02:00:10',NULL,'queued',1,'PythonOperator',0,'host3','/logs/dag_3/task_b.log'),
('dag_3','task_c','scheduled__2025-11-01_02','2025-11-01 02:00:15',NULL,'queued',1,'PythonOperator',0,'host3','/logs/dag_3/task_c.log'),

-- DAG 4
('dag_4','task_a','manual__2025-11-01','2025-11-01 06:00:00','2025-11-01 06:05:00','success',1,'PythonOperator',300,'host4','/logs/dag_4/task_a.log'),
('dag_4','task_b','manual__2025-11-01','2025-11-01 06:05:05','2025-11-01 06:10:00','success',1,'PythonOperator',295,'host4','/logs/dag_4/task_b.log'),
('dag_4','task_c','manual__2025-11-01','2025-11-01 06:10:10','2025-11-01 06:15:00','success',1,'PythonOperator',290,'host4','/logs/dag_4/task_c.log'),
('dag_4','task_a','scheduled__2025-11-02','2025-11-02 06:00:01',NULL,'running',1,'PythonOperator',0,'host4','/logs/dag_4/task_a.log'),
('dag_4','task_b','scheduled__2025-11-02','2025-11-02 06:00:05',NULL,'queued',1,'PythonOperator',0,'host4','/logs/dag_4/task_b.log'),
('dag_4','task_c','scheduled__2025-11-02','2025-11-02 06:00:10',NULL,'queued',1,'PythonOperator',0,'host4','/logs/dag_4/task_c.log'),

-- DAG 5
('dag_5','task_a','manual__2025-11-01','2025-11-01 18:00:00','2025-11-01 18:05:00','success',1,'PythonOperator',300,'host5','/logs/dag_5/task_a.log'),
('dag_5','task_b','manual__2025-11-01','2025-11-01 18:05:05','2025-11-01 18:10:00','success',1,'PythonOperator',295,'host5','/logs/dag_5/task_b.log'),
('dag_5','task_c','manual__2025-11-01','2025-11-01 18:10:10','2025-11-01 18:15:00','success',1,'PythonOperator',290,'host5','/logs/dag_5/task_c.log'),
('dag_5','task_a','scheduled__2025-11-02','2025-11-02 18:00:01',NULL,'running',1,'PythonOperator',0,'host5','/logs/dag_5/task_a.log'),
('dag_5','task_b','scheduled__2025-11-02','2025-11-02 18:00:05',NULL,'queued',1,'PythonOperator',0,'host5','/logs/dag_5/task_b.log'),
('dag_5','task_c','scheduled__2025-11-02','2025-11-02 18:00:10',NULL,'queued',1,'PythonOperator',0,'host5','/logs/dag_5/task_c.log'),

-- DAG 6
('dag_6','task_a','manual__2025-11-01','2025-11-01 00:00:00','2025-11-01 00:05:00','success',1,'PythonOperator',300,'host6','/logs/dag_6/task_a.log'),
('dag_6','task_b','manual__2025-11-01','2025-11-01 00:05:05','2025-11-01 00:10:00','success',1,'PythonOperator',295,'host6','/logs/dag_6/task_b.log'),
('dag_6','task_c','manual__2025-11-01','2025-11-01 00:10:10','2025-11-01 00:15:00','success',1,'PythonOperator',290,'host6','/logs/dag_6/task_c.log'),
('dag_6','task_a','scheduled__2025-11-08','2025-11-08 00:00:01',NULL,'running',1,'PythonOperator',0,'host6','/logs/dag_6/task_a.log'),
('dag_6','task_b','scheduled__2025-11-08','2025-11-08 00:00:05',NULL,'queued',1,'PythonOperator',0,'host6','/logs/dag_6/task_b.log'),
('dag_6','task_c','scheduled__2025-11-08','2025-11-08 00:00:10',NULL,'queued',1,'PythonOperator',0,'host6','/logs/dag_6/task_c.log'),

-- DAG 7
('dag_7','task_a','manual__2025-11-01','2025-11-01 03:00:00','2025-11-01 03:05:00','success',1,'PythonOperator',300,'host7','/logs/dag_7/task_a.log'),
('dag_7','task_b','manual__2025-11-01','2025-11-01 03:05:05','2025-11-01 03:10:00','success',1,'PythonOperator',295,'host7','/logs/dag_7/task_b.log'),
('dag_7','task_c','manual__2025-11-01','2025-11-01 03:10:10','2025-11-01 03:15:00','success',1,'PythonOperator',290,'host7','/logs/dag_7/task_c.log'),
('dag_7','task_a','scheduled__2025-11-02','2025-11-02 03:00:01',NULL,'running',1,'PythonOperator',0,'host7','/logs/dag_7/task_a.log'),
('dag_7','task_b','scheduled__2025-11-02','2025-11-02 03:00:05',NULL,'queued',1,'PythonOperator',0,'host7','/logs/dag_7/task_b.log'),
('dag_7','task_c','scheduled__2025-11-02','2025-11-02 03:00:10',NULL,'queued',1,'PythonOperator',0,'host7','/logs/dag_7/task_c.log'),

-- DAG 8
('dag_8','task_a','manual__2025-11-01','2025-11-01 00:00:00','2025-11-01 00:05:00','success',1,'PythonOperator',300,'host8','/logs/dag_8/task_a.log'),
('dag_8','task_b','manual__2025-11-01','2025-11-01 00:05:05','2025-11-01 00:10:00','success',1,'PythonOperator',295,'host8','/logs/dag_8/task_b.log'),
('dag_8','task_c','manual__2025-11-01','2025-11-01 00:10:10','2025-11-01 00:15:00','success',1,'PythonOperator',290,'host8','/logs/dag_8/task_c.log'),
('dag_8','task_a','scheduled__2025-11-02','2025-11-02 00:00:01',NULL,'running',1,'PythonOperator',0,'host8','/logs/dag_8/task_a.log'),
('dag_8','task_b','scheduled__2025-11-02','2025-11-02 00:00:05',NULL,'queued',1,'PythonOperator',0,'host8','/logs/dag_8/task_b.log'),
('dag_8','task_c','scheduled__2025-11-02','2025-11-02 00:00:10',NULL,'queued',1,'PythonOperator',0,'host8','/logs/dag_8/task_c.log'),

-- DAG 9
('dag_9','task_a','manual__2025-11-01','2025-11-01 09:30:00','2025-11-01 09:35:00','success',1,'PythonOperator',300,'host9','/logs/dag_9/task_a.log'),
('dag_9','task_b','manual__2025-11-01','2025-11-01 09:35:05','2025-11-01 09:40:00','success',1,'PythonOperator',295,'host9','/logs/dag_9/task_b.log'),
('dag_9','task_c','manual__2025-11-01','2025-11-01 09:40:10','2025-11-01 09:45:00','success',1,'PythonOperator',290,'host9','/logs/dag_9/task_c.log'),
('dag_9','task_a','scheduled__2025-11-02','2025-11-02 09:30:01',NULL,'running',1,'PythonOperator',0,'host9','/logs/dag_9/task_a.log'),
('dag_9','task_b','scheduled__2025-11-02','2025-11-02 09:30:05',NULL,'queued',1,'PythonOperator',0,'host9','/logs/dag_9/task_b.log'),
('dag_9','task_c','scheduled__2025-11-02','2025-11-02 09:30:10',NULL,'queued',1,'PythonOperator',0,'host9','/logs/dag_9/task_c.log'),

-- DAG 10
('dag_10','task_a','manual__2025-11-01','2025-11-01 00:00:00','2025-11-01 00:20:00','success',1,'PythonOperator',1200,'host10','/logs/dag_10/task_a.log'),
('dag_10','task_b','manual__2025-11-01','2025-11-01 00:05:05','2025-11-01 00:25:00','success',1,'PythonOperator',1200,'host10','/logs/dag_10/task_b.log'),
('dag_10','task_c','manual__2025-11-01','2025-11-01 00:10:10','2025-11-01 00:30:00','success',1,'PythonOperator',1200,'host10','/logs/dag_10/task_c.log'),
('dag_10','task_a','scheduled__2025-11-02','2025-11-02 00:00:01',NULL,'running',1,'PythonOperator',0,'host10','/logs/dag_10/task_a.log'),
('dag_10','task_b','scheduled__2025-11-02','2025-11-02 00:00:05',NULL,'queued',1,'PythonOperator',0,'host10','/logs/dag_10/task_b.log'),
('dag_10','task_c','scheduled__2025-11-02','2025-11-02 00:00:10',NULL,'queued',1,'PythonOperator',0,'host10','/logs/dag_10/task_c.log');

-- logs

INSERT INTO LOG (dag_id, task_id, run_id, try_number, event, when, message)
VALUES
-- DAG 1
('dag_1','task_a','manual__2025-11-01',1,'Task Started','2025-11-01 12:01:00','Task task_a started execution.'),
('dag_1','task_a','manual__2025-11-01',1,'Task Success','2025-11-01 12:02:00','Task task_a completed successfully.'),
('dag_1','task_b','manual__2025-11-01',1,'Task Started','2025-11-01 12:02:05','Task task_b started execution.'),
('dag_1','task_b','manual__2025-11-01',1,'Task Success','2025-11-01 12:03:00','Task task_b completed successfully.'),
('dag_1','task_c','manual__2025-11-01',1,'Task Started','2025-11-01 12:03:10','Task task_c started execution.'),
('dag_1','task_c','manual__2025-11-01',1,'Task Success','2025-11-01 12:05:00','Task task_c completed successfully.'),

('dag_1','task_a','scheduled__2025-11-02',1,'Task Started','2025-11-02 12:00:10','Task task_a started execution.'),
('dag_1','task_a','scheduled__2025-11-02',1,'Task Running','2025-11-02 12:00:30','Task task_a is running.'),
('dag_1','task_b','scheduled__2025-11-02',1,'Task Started','2025-11-02 12:00:15','Task task_b started execution.'),
('dag_1','task_b','scheduled__2025-11-02',1,'Task Queued','2025-11-02 12:00:35','Task task_b is queued.'),
('dag_1','task_c','scheduled__2025-11-02',1,'Task Started','2025-11-02 12:00:20','Task task_c started execution.'),
('dag_1','task_c','scheduled__2025-11-02',1,'Task Queued','2025-11-02 12:00:40','Task task_c is queued.'),

-- DAG 2
('dag_2','task_a','manual__2025-11-03',1,'Task Started','2025-11-03 00:01:00','Task task_a started execution.'),
('dag_2','task_a','manual__2025-11-03',1,'Task Failed','2025-11-03 00:05:00','Task task_a failed due to error.'),
('dag_2','task_b','manual__2025-11-03',1,'Task Started','2025-11-03 00:05:05','Task task_b started execution.'),
('dag_2','task_b','manual__2025-11-03',1,'Task Skipped','2025-11-03 00:08:00','Task task_b skipped.'),
('dag_2','task_c','manual__2025-11-03',1,'Task Started','2025-11-03 00:08:10','Task task_c started execution.'),
('dag_2','task_c','manual__2025-11-03',1,'Task Skipped','2025-11-03 00:10:00','Task task_c skipped.'),

('dag_2','task_a','scheduled__2025-11-10',1,'Task Started','2025-11-10 00:00:05','Task task_a started execution.'),
('dag_2','task_a','scheduled__2025-11-10',1,'Task Success','2025-11-10 00:08:00','Task task_a completed successfully.'),
('dag_2','task_b','scheduled__2025-11-10',1,'Task Started','2025-11-10 00:00:10','Task task_b started execution.'),
('dag_2','task_b','scheduled__2025-11-10',1,'Task Success','2025-11-10 00:08:00','Task task_b completed successfully.'),
('dag_2','task_c','scheduled__2025-11-10',1,'Task Started','2025-11-10 00:00:15','Task task_c started execution.'),
('dag_2','task_c','scheduled__2025-11-10',1,'Task Success','2025-11-10 00:08:00','Task task_c completed successfully.'),

-- DAG 3
('dag_3','task_a','manual__2025-11-07',1,'Task Started','2025-11-07 00:01:00','Task task_a started execution.'),
('dag_3','task_a','manual__2025-11-07',1,'Task Failed','2025-11-07 00:05:00','Task task_a failed due to error.'),
('dag_3','task_b','manual__2025-11-07',1,'Task Started','2025-11-07 00:05:05','Task task_b started execution.'),
('dag_3','task_b','manual__2025-11-07',1,'Task Skipped','2025-11-07 00:08:00','Task task_b skipped.'),
('dag_3','task_c','manual__2025-11-07',1,'Task Started','2025-11-07 00:08:10','Task task_c started execution.'),
('dag_3','task_c','manual__2025-11-07',1,'Task Skipped','2025-11-07 00:10:00','Task task_c skipped.'),

('dag_3','task_a','scheduled__2025-11-09',1,'Task Started','2025-11-09 00:00:05','Task task_a started execution.'),
('dag_3','task_a','scheduled__2025-11-09',1,'Task Success','2025-11-09 00:08:00','Task task_a completed successfully.'),
('dag_3','task_b','scheduled__2025-11-09',1,'Task Started','2025-11-09 00:00:10','Task task_b started execution.'),
('dag_3','task_b','scheduled__2025-11-09',1,'Task Success','2025-11-09 00:08:00','Task task_b completed successfully.'),
('dag_3','task_c','scheduled__2025-11-09',1,'Task Started','2025-11-09 00:00:15','Task task_c started execution.'),
('dag_3','task_c','scheduled__2025-11-09',1,'Task Success','2025-11-09 00:08:00','Task task_c completed successfully.'),

--DAG 4
('dag_4','task_a','manual__2025-11-05',1,'Task Started','2025-11-05 00:01:00','Task task_a started execution.'),
('dag_4','task_a','manual__2025-11-05',1,'Task Failed','2025-11-05 00:05:00','Task task_a failed due to error.'),
('dag_4','task_b','manual__2025-11-05',1,'Task Started','2025-11-05 00:05:05','Task task_b started execution.'),
('dag_4','task_b','manual__2025-11-05',1,'Task Skipped','2025-11-05 00:08:00','Task task_b skipped.'),
('dag_4','task_c','manual__2025-11-05',1,'Task Started','2025-11-05 00:08:10','Task task_c started execution.'),
('dag_4','task_c','manual__2025-11-05',1,'Task Skipped','2025-11-05 00:10:00','Task task_c skipped.'),

('dag_4','task_a','scheduled__2025-11-10',1,'Task Started','2025-11-08 00:00:05','Task task_a started execution.'),
('dag_4','task_a','scheduled__2025-11-10',1,'Task Success','2025-11-08 00:08:00','Task task_a completed successfully.'),
('dag_4','task_b','scheduled__2025-11-10',1,'Task Started','2025-11-08 00:00:10','Task task_b started execution.'),
('dag_4','task_b','scheduled__2025-11-10',1,'Task Success','2025-11-08 00:08:00','Task task_b completed successfully.'),
('dag_4','task_c','scheduled__2025-11-10',1,'Task Started','2025-11-08 00:00:15','Task task_c started execution.'),
('dag_4','task_c','scheduled__2025-11-10',1,'Task Success','2025-11-08 00:08:00','Task task_c completed successfully.'),

--DAG 5
('dag_5','task_a','manual__2025-11-13',1,'Task Started','2025-11-13 00:01:00','Task task_a started execution.'),
('dag_5','task_a','manual__2025-11-13',1,'Task Failed','2025-11-13 00:05:00','Task task_a failed due to error.'),
('dag_5','task_b','manual__2025-11-13',1,'Task Started','2025-11-13 00:05:05','Task task_b started execution.'),
('dag_5','task_b','manual__2025-11-13',1,'Task Skipped','2025-11-13 00:08:00','Task task_b skipped.'),
('dag_5','task_c','manual__2025-11-13',1,'Task Started','2025-11-13 00:08:10','Task task_c started execution.'),
('dag_5','task_c','manual__2025-11-13',1,'Task Skipped','2025-11-13 00:10:00','Task task_c skipped.'),

('dag_5','task_a','scheduled__2025-11-14',1,'Task Started','2025-11-14 00:00:05','Task task_a started execution.'),
('dag_5','task_a','scheduled__2025-11-14',1,'Task Success','2025-11-14 00:08:00','Task task_a completed successfully.'),
('dag_5','task_b','scheduled__2025-11-14',1,'Task Started','2025-11-14 00:00:10','Task task_b started execution.'),
('dag_5','task_b','scheduled__2025-11-14',1,'Task Success','2025-11-14 00:08:00','Task task_b completed successfully.'),
('dag_5','task_c','scheduled__2025-11-14',1,'Task Started','2025-11-14 00:00:15','Task task_c started execution.'),
('dag_5','task_c','scheduled__2025-11-14',1,'Task Success','2025-11-14 00:08:00','Task task_c completed successfully.'),

--DAG 6

('dag_6','task_a','manual__2025-11-15',1,'Task Started','2025-11-15 00:01:00','Task task_a started execution.'),
('dag_6','task_a','manual__2025-11-15',1,'Task Failed','2025-11-15 00:05:00','Task task_a failed due to error.'),
('dag_6','task_b','manual__2025-11-15',1,'Task Started','2025-11-15 00:05:05','Task task_b started execution.'),
('dag_6','task_b','manual__2025-11-15',1,'Task Skipped','2025-11-15 00:08:00','Task task_b skipped.'),
('dag_6','task_c','manual__2025-11-15',1,'Task Started','2025-11-15 00:08:10','Task task_c started execution.'),
('dag_6','task_c','manual__2025-11-15',1,'Task Skipped','2025-11-15 00:10:00','Task task_c skipped.'),

('dag_6','task_a','scheduled__2025-11-16',1,'Task Started','2025-11-16 00:00:05','Task task_a started execution.'),
('dag_6','task_a','scheduled__2025-11-16',1,'Task Success','2025-11-16 00:08:00','Task task_a completed successfully.'),
('dag_6','task_b','scheduled__2025-11-16',1,'Task Started','2025-11-16 00:00:10','Task task_b started execution.'),
('dag_6','task_b','scheduled__2025-11-16',1,'Task Success','2025-11-16 00:08:00','Task task_b completed successfully.'),
('dag_6','task_c','scheduled__2025-11-16',1,'Task Started','2025-11-16 00:00:15','Task task_c started execution.'),
('dag_6','task_c','scheduled__2025-11-16',1,'Task Success','2025-11-16 00:08:00','Task task_c completed successfully.'),

-- DAG 7

('dag_7','task_a','manual__2025-11-17',1,'Task Started','2025-11-17 00:01:00','Task task_a started execution.'),
('dag_7','task_a','manual__2025-11-17',1,'Task Failed','2025-11-17 00:05:00','Task task_a failed due to error.'),
('dag_7','task_b','manual__2025-11-17',1,'Task Started','2025-11-17 00:05:05','Task task_b started execution.'),
('dag_7','task_b','manual__2025-11-17',1,'Task Skipped','2025-11-17 00:08:00','Task task_b skipped.'),
('dag_7','task_c','manual__2025-11-17',1,'Task Started','2025-11-17 00:08:10','Task task_c started execution.'),
('dag_7','task_c','manual__2025-11-17',1,'Task Skipped','2025-11-17 00:10:00','Task task_c skipped.'),

('dag_7','task_a','scheduled__2025-11-18',1,'Task Started','2025-11-18 00:00:05','Task task_a started execution.'),
('dag_7','task_a','scheduled__2025-11-18',1,'Task Success','2025-11-18 00:08:00','Task task_a completed successfully.'),
('dag_7','task_b','scheduled__2025-11-18',1,'Task Started','2025-11-18 00:00:10','Task task_b started execution.'),
('dag_7','task_b','scheduled__2025-11-18',1,'Task Success','2025-11-18 00:08:00','Task task_b completed successfully.'),
('dag_7','task_c','scheduled__2025-11-18',1,'Task Started','2025-11-18 00:00:15','Task task_c started execution.'),
('dag_7','task_c','scheduled__2025-11-18',1,'Task Success','2025-11-18 00:08:00','Task task_c completed successfully.'),

-- DAG 8

('dag_8','task_a','manual__2025-11-19',1,'Task Started','2025-11-19 00:01:00','Task task_a started execution.'),
('dag_8','task_a','manual__2025-11-19',1,'Task Failed','2025-11-19 00:05:00','Task task_a failed due to error.'),
('dag_8','task_b','manual__2025-11-19',1,'Task Started','2025-11-19 00:05:05','Task task_b started execution.'),
('dag_8','task_b','manual__2025-11-19',1,'Task Skipped','2025-11-19 00:08:00','Task task_b skipped.'),
('dag_8','task_c','manual__2025-11-19',1,'Task Started','2025-11-19 00:08:10','Task task_c started execution.'),
('dag_8','task_c','manual__2025-11-19',1,'Task Skipped','2025-11-19 00:10:00','Task task_c skipped.'),

('dag_8','task_a','scheduled__2025-11-20',1,'Task Started','2025-11-20 00:00:05','Task task_a started execution.'),
('dag_8','task_a','scheduled__2025-11-20',1,'Task Success','2025-11-20 00:08:00','Task task_a completed successfully.'),
('dag_8','task_b','scheduled__2025-11-20',1,'Task Started','2025-11-20 00:00:10','Task task_b started execution.'),
('dag_8','task_b','scheduled__2025-11-20',1,'Task Success','2025-11-20 00:08:00','Task task_b completed successfully.'),
('dag_8','task_c','scheduled__2025-11-20',1,'Task Started','2025-11-20 00:00:15','Task task_c started execution.'),
('dag_8','task_c','scheduled__2025-11-20',1,'Task Success','2025-11-20 00:08:00','Task task_c completed successfully.'),

-- DAG 9

('dag_9','task_a','manual__2025-11-21',1,'Task Started','2025-11-21 00:01:00','Task task_a started execution.'),
('dag_9','task_a','manual__2025-11-21',1,'Task Failed','2025-11-21 00:05:00','Task task_a failed due to error.'),
('dag_9','task_b','manual__2025-11-21',1,'Task Started','2025-11-21 00:05:05','Task task_b started execution.'),
('dag_9','task_b','manual__2025-11-21',1,'Task Skipped','2025-11-21 00:08:00','Task task_b skipped.'),
('dag_9','task_c','manual__2025-11-21',1,'Task Started','2025-11-21 00:08:10','Task task_c started execution.'),
('dag_9','task_c','manual__2025-11-21',1,'Task Skipped','2025-11-21 00:10:00','Task task_c skipped.'),

('dag_9','task_a','scheduled__2025-11-22',1,'Task Started','2025-11-22 00:00:05','Task task_a started execution.'),
('dag_9','task_a','scheduled__2025-11-22',1,'Task Success','2025-11-22 00:08:00','Task task_a completed successfully.'),
('dag_9','task_b','scheduled__2025-11-22',1,'Task Started','2025-11-22 00:00:10','Task task_b started execution.'),
('dag_9','task_b','scheduled__2025-11-22',1,'Task Success','2025-11-22 00:08:00','Task task_b completed successfully.'),
('dag_9','task_c','scheduled__2025-11-22',1,'Task Started','2025-11-22 00:00:15','Task task_c started execution.'),
('dag_9','task_c','scheduled__2025-11-22',1,'Task Success','2025-11-22 00:08:00','Task task_c completed successfully.'),

-- DAG 10

('dag_10','task_a','manual__2025-11-23',1,'Task Started','2025-11-23 00:01:00','Task task_a started execution.'),
('dag_10','task_a','manual__2025-11-23',1,'Task Failed','2025-11-23 00:05:00','Task task_a failed due to error.'),
('dag_10','task_b','manual__2025-11-23',1,'Task Started','2025-11-23 00:05:05','Task task_b started execution.'),
('dag_10','task_b','manual__2025-11-23',1,'Task Skipped','2025-11-23 00:08:00','Task task_b skipped.'),
('dag_10','task_c','manual__2025-11-23',1,'Task Started','2025-11-23 00:08:10','Task task_c started execution.'),
('dag_10','task_c','manual__2025-11-23',1,'Task Skipped','2025-11-23 00:10:00','Task task_c skipped.'),

('dag_10','task_a','scheduled__2025-11-24',1,'Task Started','2025-11-24 00:00:05','Task task_a started execution.'),
('dag_10','task_a','scheduled__2025-11-24',1,'Task Success','2025-11-24 00:08:00','Task task_a completed successfully.'),
('dag_10','task_b','scheduled__2025-11-24',1,'Task Started','2025-11-24 00:00:10','Task task_b started execution.'),
('dag_10','task_b','scheduled__2025-11-24',1,'Task Success','2025-11-24 00:08:00','Task task_b completed successfully.'),
('dag_10','task_c','scheduled__2025-11-24',1,'Task Started','2025-11-24 00:00:15','Task task_c started execution.'),
('dag_10','task_c','scheduled__2025-11-24',1,'Task Success','2025-11-24 00:08:00','Task task_c completed successfully.');


INSERT INTO VARIABLE (key, value)
VALUES
('env','development'),
('max_retries','3'),
('default_owner','alice'),
('notification_email','team@example.com');

INSERT INTO CONNECTION (conn_id, conn_type, host, schema, login, password, port, extra)
VALUES
('postgres_dev','postgres','localhost','public','user1','pass123',5432,'{}'),
('snowflake_dev','snowflake','account.snowflakecomputing.com','PI_FLOW','sf_user','sf_pass',443,'{"warehouse":"DEV_WAREHOUSE"}'),
('aws_s3','s3','s3.amazonaws.com','','access_key','secret_key',NULL,'{"region":"us-east-1"}');



ALTER TABLE JOB ADD COLUMN run_id STRING;

ALTER TABLE JOB 
ADD COLUMN latest_heartbeat TIMESTAMP,
 COLUMN executor_class STRING;



INSERT INTO JOB (id, dag_id, run_id, state, job_type, start_date, end_date, latest_heartbeat, executor_class, hostname)
VALUES
-- DAG 1
(1, 'dag_1', 'manual__2025-11-01', 'success', 'scheduler', '2025-11-01 12:00:00', '2025-11-01 12:05:00', '2025-11-01 12:05:00', 'SequentialExecutor', 'host1'),
(2, 'dag_1', 'scheduled__2025-11-02', 'running', 'scheduler', '2025-11-02 12:00:00', NULL, '2025-11-02 12:01:00', 'SequentialExecutor', 'host1'),

-- DAG 2
(3, 'dag_2', 'manual__2025-11-03', 'failed', 'scheduler', '2025-11-03 00:00:00', '2025-11-03 00:10:00', '2025-11-03 00:10:00', 'SequentialExecutor', 'host2'),
(4, 'dag_2', 'scheduled__2025-11-10', 'success', 'scheduler', '2025-11-10 00:00:00', '2025-11-10 00:08:00', '2025-11-10 00:08:00', 'SequentialExecutor', 'host2'),

-- DAG 3
(5, 'dag_3', 'manual__2025-11-01_01', 'success', 'scheduler', '2025-11-01 01:00:00', '2025-11-01 01:01:00', '2025-11-01 01:01:00', 'SequentialExecutor', 'host3'),
(6, 'dag_3', 'scheduled__2025-11-01_02', 'success', 'scheduler', '2025-11-01 02:00:00', '2025-11-01 02:01:00', '2025-11-01 02:01:00', 'SequentialExecutor', 'host3'),

-- DAG 4
(7, 'dag_4', 'manual__2025-11-01', 'running', 'scheduler', '2025-11-01 06:00:00', NULL, '2025-11-01 06:01:00', 'SequentialExecutor', 'host4'),
(8, 'dag_4', 'scheduled__2025-11-02', 'success', 'scheduler', '2025-11-02 06:00:00', '2025-11-02 06:03:00', '2025-11-02 06:03:00', 'SequentialExecutor', 'host4'),

-- DAG 5
(9, 'dag_5', 'manual__2025-11-01', 'success', 'scheduler', '2025-11-01 18:00:00', '2025-11-01 18:10:00', '2025-11-01 18:10:00', 'SequentialExecutor', 'host5'),
(10, 'dag_5', 'scheduled__2025-11-02', 'running', 'scheduler', '2025-11-02 18:00:00', NULL, '2025-11-02 18:01:00', 'SequentialExecutor', 'host5'),

-- DAG 6
(11, 'dag_6', 'manual__2025-11-01', 'success', 'scheduler', '2025-11-01 00:00:00', '2025-11-01 00:10:00', '2025-11-01 00:10:00', 'SequentialExecutor', 'host6'),
(12, 'dag_6', 'scheduled__2025-11-08', 'running', 'scheduler', '2025-11-08 00:00:00', NULL, '2025-11-08 00:01:00', 'SequentialExecutor', 'host6'),

-- DAG 7
(13, 'dag_7', 'manual__2025-11-01', 'success', 'scheduler', '2025-11-01 03:00:00', '2025-11-01 03:10:00', '2025-11-01 03:10:00', 'SequentialExecutor', 'host7'),
(14, 'dag_7', 'scheduled__2025-11-02', 'running', 'scheduler', '2025-11-02 03:00:00', NULL, '2025-11-02 03:01:00', 'SequentialExecutor', 'host7'),

-- DAG 8
(15, 'dag_8', 'manual__2025-11-01', 'success', 'scheduler', '2025-11-01 00:00:00', '2025-11-01 00:10:00', '2025-11-01 00:10:00', 'SequentialExecutor', 'host8'),
(16, 'dag_8', 'scheduled__2025-11-02', 'running', 'scheduler', '2025-11-02 00:00:00', NULL, '2025-11-02 00:01:00', 'SequentialExecutor', 'host8'),

-- DAG 9
(17, 'dag_9', 'manual__2025-11-01', 'success', 'scheduler', '2025-11-01 09:30:00', '2025-11-01 09:45:00', '2025-11-01 09:45:00', 'SequentialExecutor', 'host9'),
(18, 'dag_9', 'scheduled__2025-11-02', 'running', 'scheduler', '2025-11-02 09:30:00', NULL, '2025-11-02 09:31:00', 'SequentialExecutor', 'host9'),

-- DAG 10
(19, 'dag_10', 'manual__2025-11-01', 'success', 'scheduler', '2025-11-01 00:00:00', '2025-11-01 00:20:00', '2025-11-01 00:20:00', 'SequentialExecutor', 'host10'),
(20, 'dag_10', 'scheduled__2025-11-02', 'running', 'scheduler', '2025-11-02 00:00:00', NULL, '2025-11-02 00:01:00', 'SequentialExecutor', 'host10');





-- Insert mock data in TASK and TASK_DEPENDENCY

INSERT INTO PI_FLOW.METADATA.TASK (
    DAG_ID,
    TASK_ID,
    OPERATOR,
    OWNER,
    DESCRIPTION,
    RETRIES,
    RETRY_DELAY_SECONDS
)
VALUES
('dag_1', 'extract',   'PythonOperator', 'data_team', 'Extract raw data', 2, 300),
('dag_1', 'transform', 'PythonOperator', 'data_team', 'Transform data',   1, 300),
('dag_1', 'load',      'SnowflakeOperator', 'data_team', 'Load data',     0, NULL),
('dag_1', 'notify',    'EmailOperator',  'data_team', 'Send notification',0, NULL);


INSERT INTO PI_FLOW.METADATA.TASK (
    DAG_ID,
    TASK_ID,
    OPERATOR,
    OWNER,
    DESCRIPTION,
    RETRIES,
    RETRY_DELAY_SECONDS,
    PARAMS
)
SELECT
    'dag_1',
    'extract',
    'snowflake',
    'data_team',
    'Execute extract stored procedure',
    2,
    300,
    OBJECT_CONSTRUCT(
        'connection_id', 'snowflake_default',
        'sql', 'CALL PI_FLOW.RAW.EXTRACT_DATA();'
    );


CREATE SCHEMA IF NOT EXISTS PI_FLOW.RAW;

CREATE OR REPLACE TABLE PI_FLOW.RAW.LOGS (
    EVENT_TIME     TIMESTAMP_NTZ NOT NULL,
    TASK_NAME      STRING        NOT NULL,
    STATUS         STRING        NOT NULL
);


CREATE OR REPLACE PROCEDURE PI_FLOW.RAW.EXTRACT_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO PI_FLOW.RAW.LOGS
    VALUES (CURRENT_TIMESTAMP(), 'EXTRACT', 'SUCCESS');

    RETURN 'OK';
END;
$$;

CREATE OR REPLACE PROCEDURE PI_FLOW.RAW.TRANSFORM_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO PI_FLOW.RAW.LOGS
    VALUES (CURRENT_TIMESTAMP(), 'TRANSFORM', 'SUCCESS');

    RETURN 'OK';
END;
$$;

CREATE OR REPLACE PROCEDURE PI_FLOW.RAW.LOAD_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO PI_FLOW.RAW.LOGS
    VALUES (CURRENT_TIMESTAMP(), 'LOAD', 'SUCCESS');

    RETURN 'OK';
END;
$$;

CREATE OR REPLACE PROCEDURE PI_FLOW.RAW.NOTIFY_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO PI_FLOW.RAW.LOGS
    VALUES (CURRENT_TIMESTAMP(), 'NOTIFY', 'SUCCESS');

    RETURN 'OK';
END;
$$;


UPDATE PI_FLOW.METADATA.TASK_INSTANCE SET STATE = 'scheduled' WHERE TASK_ID ='extract';

DELETE FROM PI_FLOW.RAW.LOGS;

INSERT INTO PI_FLOW.METADATA.TASK_DEPENDENCY (
    DAG_ID,
    UPSTREAM_TASK_ID,
    DOWNSTREAM_TASK_ID
)
VALUES
('dag_1', 'extract',   'transform'),
('dag_1', 'transform', 'load'),
('dag_1', 'transform', 'notify');





show warehouses;

alter warehouse "COMPUTE_WH" suspend;

UPDATE DAG_RUN
SET run_id = dag_id || '_' || run_id;



update pi_flow.metadata.dag_run
SET START_DATE=NULL,
END_DATE=NULL;

update dag_run 
set run_id = run_id || '09:12:03'
where id<=10;

UPDATE DAG_RUN
SET run_id = 
    SUBSTR(run_id, 1, 24) || '_' || SUBSTR(run_id, 25)
WHERE id <= 10
and run_type='scheduled';

UPDATE DAG_RUN
SET run_id='dag_temp_manual__2025-12-17_10:14:17'
where id=101;

select * from pi_flow.metadata.dag_run; 

show warehouses;


alter warehouse "COMPUTE_WH" resume;

select * from pi_flow.metadata.dag 
where tags is not null
limit 5;

show tables;

create temp table names(name varchar);

select * from connection;

SHOW WAREHOUSES LIKE 'COMPUTE_WH';




