# Kendo


## Setup

Along side the dependencies, this will also install the CLI inside the virtual environment.
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
$ kendo --help
$ kendo snowflake --help
$ kendo snowflake session-details
```

