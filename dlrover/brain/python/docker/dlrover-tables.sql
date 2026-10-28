create database dlrover;

use dlrover;

create table job_metrics(
    uid varchar(128) NOT NULL, 
    job_name varchar(128),
    job_id varchar(128), 
    cluster varchar(128),
    namespace varchar(128),   
    job_feature mediumtext,
    training_set_feature mediumtext, 
    model_feature mediumtext,
    job_runtime mediumtext,
    exit_reason varchar(255),
    optimization mediumtext,
    resource mediumtext,
    customized_data mediumtext,
    type varchar(255),
    extra_info mediumtext,
    created_at datetime,
    PRIMARY KEY (uid)
);

create table job(
    uid varchar(128) NOT NULL,
    job_name varchar(128),
    job_id varchar(128), 
    scenario varchar(128),
    created_at timestamp,
    started_at timestamp,
    finished_at timestamp,
    status mediumtext,
    PRIMARY KEY (uid)
);

create table job_node(
    uid varchar(128) NOT NULL,
    job_name varchar(128),
    job_id varchar(128), 
    type varchar(64),
    created_at timestamp,
    started_at timestamp,
    finished_at timestamp,
    resource mediumtext,
    status mediumtext,
    customized_data mediumtext,
    PRIMARY KEY (uid)
);

create table cluster(
    uid varchar(255) NOT NULL,
    name varchar(255),
    resources mediumtext,
    status mediumtext,
    customized_data mediumtext,
    PRIMARY KEY (uid)
);

create table gpu_tw_metric_records(
    uid varchar(255) NOT NULL, 
    job_name varchar(255),
    job_id varchar(255), 
    cluster varchar(64),  
    namespace varchar(64), 
    pod_name varchar(255),
    record_timestamp bigint,
    max_gpu_utilization float,
    max_gpu_used_memory_mb bigint,
    max_gpu_memory_utilization float,
    gpu_count int,
    raw_utils mediumtext,
    raw_gpu_used_memory mediumtext,
    raw_gpu_memory_utils mediumtext,
    created_at datetime,
    PRIMARY KEY (uid)
);

create table gpu_tw_metric_decision_records(
    uid varchar(128) NOT NULL, 
    job_name varchar(128),
    job_id varchar(128), 
    cluster varchar(64), 
    namespace varchar(64), 
    pod_name varchar(255),
    avg_gpu_utilization float,
    avg_gpu_memory_utilization float,
    sample_count int,
    elastic_action varchar(32),
    emergency_trigger tinyint(1),
    emergency_trigger_time datetime,
    created_at datetime,
    PRIMARY KEY (uid)
);

