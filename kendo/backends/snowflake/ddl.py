SQL = """
CREATE DATABASE IF NOT EXISTS kendo_db;
CREATE SCHEMA IF NOT EXISTS kendo_db.config;
CREATE TABLE IF NOT EXISTS kendo_db.config.tags (
    id INT PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS kendo_db.config.tags_allowed_values (
    id INT PRIMARY KEY AUTOINCREMENT,
    tag_id INT NOT NULL,
    value VARCHAR(500) NOT NULL,
    FOREIGN KEY (tag_id) REFERENCES kendo_db.config.tags(id)
);
CREATE TABLE IF NOT EXISTS kendo_db.config.tags_assignments (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_type VARCHAR(255) NOT NULL,
    obj_path VARCHAR NOT NULL,
    tag_id INT NOT NULL,
    value VARCHAR(500) NOT NULL,
    FOREIGN KEY (tag_id) REFERENCES kendo_db.config.tags(id)
);
CREATE SCHEMA IF NOT EXISTS kendo_db.infrastructure;
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.database_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.schema_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL,
    database_id INT NOT NULL,
    FOREIGN KEY (database_id) REFERENCES kendo_db.infrastructure.database_objs(id)
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.table_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL,
    schema_id INT NOT NULL,
    kind VARCHAR NULL,
    comment VARCHAR NULL,
    cluster_by VARCHAR NULL,
    num_rows INT NULL,
    num_bytes INT NULL,
    owner_role_id INT NULL,
    retention_time VARCHAR NULL,
    automatic_clustering VARCHAR NULL,
    change_tracking VARCHAR NULL,
    is_external VARCHAR NULL,
    enable_schema_evolution VARCHAR NULL,
    owner_role_type VARCHAR NULL,
    is_event VARCHAR NULL,
    budget VARCHAR NULL,
    is_hybrid VARCHAR NULL,
    is_iceberg VARCHAR NULL,
    is_dynamic VARCHAR NULL,
    ddl VARCHAR NULL,
    FOREIGN KEY (schema_id) REFERENCES kendo_db.infrastructure.schema_objs(id)
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.stage_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL,
    schema_id INT NOT NULL,
    FOREIGN KEY (schema_id) REFERENCES kendo_db.infrastructure.schema_objs(id)
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.stream_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL,
    schema_id INT NOT NULL,
    table_name VARCHAR NULL,
    FOREIGN KEY (schema_id) REFERENCES kendo_db.infrastructure.schema_objs(id)
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.pipe_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL,
    schema_id INT NOT NULL,
    FOREIGN KEY (schema_id) REFERENCES kendo_db.infrastructure.schema_objs(id)
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.column_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL,
    table_id INT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES kendo_db.infrastructure.table_objs(id)
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.role_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.user_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    last_success_login TIMESTAMP_LTZ NULL,
    login_name VARCHAR NOT NULL,
    owner_role_id INT NULL,
    email VARCHAR NULL,
    default_role_id INT NULL,
    ext_authn_uid VARCHAR NULL,
    is_ext_authn_duo BOOLEAN NULL
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.grants_privilege_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    privilege VARCHAR NOT NULL,
    granted_on VARCHAR NOT NULL,
    granted_on_id INT NOT NULL,
    granted_to VARCHAR NOT NULL,
    granted_to_id INT NOT NULL,
    grant_option BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.grants_role_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    role_id INT NOT NULL,
    granted_to VARCHAR NOT NULL,
    granted_to_id INT NOT NULL,
    granted_by_role_id INT NULL
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.warehouse_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    size VARCHAR NOT NULL,
    obj_created_on TIMESTAMP_LTZ NULL,
    owner_role_id INT NULL
);
"""
