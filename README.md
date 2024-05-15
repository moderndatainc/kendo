# Kendo


## Setup

Along side the dependencies, the command below will also install the CLI inside the virtual environment.
```
$ poetry install
```

Kendo looks for Snowflake credentials located in `~/.snowflake/connections.toml`. It will look for a connection called `default`.
```
[default]
account = "acc12345"
user = "youruser"
password = "yourpass"
```

## Usage

```
$ poetry shell
$ kendo --help
```

The first command that should be run is `configure`. Because that will setup a configuration file in `~/.kendo/config.toml`.
```
$ kendo configure
```

### Scan datasource infrastructure
```
$ kendo scan-infra
```

### Tags
Create a tag with any allowed values.
```
$ kendo create-tag country
```

Create a tag with specific allowed values.
```
$ kendo create-tag country US CA 'Cayman Islands'
```