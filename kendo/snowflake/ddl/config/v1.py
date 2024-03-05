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
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.role_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.grant_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    privilege VARCHAR NOT NULL,
    granted_on VARCHAR NOT NULL,
    granted_on_id INT NOT NULL,
    granted_to VARCHAR NOT NULL,
    granted_to_id INT NOT NULL,
    grant_option BOOLEAN NOT NULL
);
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
    FOREIGN KEY (schema_id) REFERENCES kendo_db.infrastructure.schema_objs(id)
);
CREATE TABLE IF NOT EXISTS kendo_db.infrastructure.column_objs (
    id INT PRIMARY KEY AUTOINCREMENT,
    obj_created_on TIMESTAMP_LTZ NULL,
    name VARCHAR NOT NULL,
    table_id INT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES kendo_db.infrastructure.table_objs(id)
);
"""
