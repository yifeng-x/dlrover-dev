create database dlrover;

use dlrover;

create table job_metrics(
    uid varchar(255) NOT NULL,
    job_name varchar(255),
    job_id varchar(255),
    namespace varchar(32),
    cluster varchar(32),
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
    PRIMARY KEY (uid)
);

create table job(
    uid varchar(255) NOT NULL,
    job_name varchar(255),
    job_id varchar(255),
    scenario varchar(255),
    created_at timestamp,
    started_at timestamp,
    finished_at timestamp,
    status mediumtext,
    PRIMARY KEY (uid)
);

create table job_node(
    uid varchar(255) NOT NULL,
    job_name varchar(255),
    job_id varchar(255),
    type varchar(255),
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
    cluster_name varchar(255),
    resources mediumtext,
    status mediumtext,
    customized_data mediumtext,
    PRIMARY KEY (uid)
);

create table gpu_tw_metric_records(
    uid varchar(128) NOT NULL,
    job_name varchar(128),
    job_id varchar(128),
    namespace varchar(32),
    pod_name varchar(128),
    timestamp integer,
    max_gpu_utilization float,
    gpu_count integer,
    raw_utils mediumtext,
    PRIMARY KEY (uid)
);

create table gpu_tw_metric_decision_records(
    uid varchar(128) NOT NULL,
    job_name varchar(128),
    job_id varchar(128),
    namespace varchar(32),
    pod_name varchar(128),
    timestamp integer,
    avg_gpu_utilization float,
    sample_count integer,
    elastic_action varchar(32),
    emergency_triggers integer,
    total_check_count integer,
    PRIMARY KEY (uid)
);


