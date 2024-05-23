# Kendo

Kendo is a testing framework designed for data infrastructure, allowing developers to define and enforce policies on their infrastructure using SQL code. Inspired by tools such as [Cloud Custodian](https://cloudcustodian.io) and [Steampipe](https://steampipe.io), Kendo enables infrastructure management through a declarative approach. 

Kendo isn't a replacement for IaC tools like Terraform, Titan or schemachange, but is designed to work in conjunction with IaC  

## Features

- Scan the entire Snowflake account to build a map of existing resources (ZeroETL) 
- Define infrastructure tests and policies using YAML.
- Execute SQL-based checks against Snowflake and other infrastructure components.
- Support for both unit-like tests and higher-level policies.
- Integrate with CI/CD pipelines for continuous validation.
- Automated actions for compliance and governance.
- Tag resources with appropriate data sensitivity labels 


## Setup

Along side the dependencies, the command below will also install the CLI inside the virtual environment.
```
$ poetry install
```

Kendo looks for Snowflake credentials located in `~/.snowflake/connections.toml`. It is set to a connection called `default`.
```
[default]
account = "acc12345"
warehouse = "transforming"
user = "youruser"
password = "yourpass"
```

## Usage

```
$ poetry shell
$ kendo --help
```

The first command that should be run is `kendo init`. This command establishes a configuration file in `~/.kendo/config.toml`.
```
$ kendo init
```

### Scan datasource infrastructure

Runs idempotent scans for all securable objects in Snowflake, and creates an internal database to store infrastructure state. Supported Snowflake objects: 

```
$ kendo scan
```


#### Resource support (format borrowed from https://github.com/Titan-Systems/titan.git)

| Name                          | Supported |
|-------------------------------|-----------|
| **Account Resources**         |           |
| API Integration               | ❌         |
| Catalog Integration           | ❌         |
| Compute Pool                  | ❌         |
| Connection                    | ❌         |
| Database                      | ✅         |
| External Access Integration   | ❌         |
| External Volume               | ❌         |
| Grant                         | ✅         |
| ↳ Future Grant                | ✅         |
| ↳ Privilege Grant             | ✅         |
| ↳ Role Grant                  | ✅         |
| Network Policy                | ❌         |
| Replication Group             | ❌         |
| Resource Monitor              | ❌         |
| Role                          | ✅         |
| Role Grant                    | ✅         |
| Security Integration          | ❌         |
| ↳ External API                | ❌         |
| ↳ External OAuth              | ❌         |
| ↳ Snowflake OAuth             | ❌         |
| ↳ SAML2                       | ❌         |
| ↳ SCIM                        | ❌         |
| Share                         | ❌         |
| User                          | ❌         |
| Warehouse                     | ✅         |
|                               |            |
| **Database Resources**        |            |
| Database Role                 | ✅         |
| Schema                        | ✅         |
|                               |            |
| **Schema Resources**          |            |
| Alert                         | ❌         |
| Aggregation Policy            | ❌         |
| Dynamic Table                 | ❌         |
| Event Table                   | ❌         |
| External Function             | ❌         |
| External Stage                | ❌         |
| External Table                | ❌         |
| Failover Group                | ❌         |
| File Format                   | ❌         |
| ↳ CSV                         | ❌         |
| ↳ JSON                        | ❌         |
| ↳ AVRO                        | ❌         |
| ↳ ORC                         | ❌         |
| ↳ Parquet                     | ❌         |
| Hybrid Table                  | ❌         |
| Iceberg Table                 | ❌         |
| Image Repository              | ❌         |
| Internal Stage                | ❌         |
| Masking Policy                | ❌         |
| Materialized View             | ❌         |
| Model                         | ❌         |
| Network Rule                  | ❌         |
| Packages Policy               | ❌         |
| Password Policy               | ❌         |
| Pipe                          | ❌         |
| Projection Policy             | ❌         |
| Row Access Policy             | ❌         |
| Secret                        | ❌         |
| Sequence                      | ❌         |
| Service                       | ❌         |
| Session Policy                | ❌         |
| Stage                         | ❌         |
| ↳ External                    | ❌         |
| ↳ Internal                    | ❌         |
| Stored Procedure              | ❌         |
| ↳ Java                        | ❌         |
| ↳ Javascript                  | ❌         |
| ↳ Python                      | ❌         |
| ↳ Scala                       | ❌         |
| ↳ SQL                         | ❌         |
| Stream                        | ✅         |
| ↳ External Table              | ❌         |
| ↳ Stage                       | ❌         |
| ↳ Table                       | ❌         |
| ↳ View                        | ✅         |
| Table                         | ✅         |
| Tag                           | ✅         |
| Task                          | ✅         |
| View                          | ❌         |

### Tags [WIP]

Create a tag with any allowed values.
```
$ kendo create-tag country
```

Create a tag with specific allowed values.
```
$ kendo create-tag country US CA 'Cayman Islands'
```

Apply tags to a specific Snowflake resource
```
$ kendo set-tag country US CA 'Cayman Islands'
```