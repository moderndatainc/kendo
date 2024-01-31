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
"""
