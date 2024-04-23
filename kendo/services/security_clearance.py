from rich import print
from kendo.datasource import SnowflakeDatasourceConnection

REQUIRED_GRANTS = [
    {
        "privilege": "CREATE DATABASE",
        "granted_on": "ACCOUNT",
    },
    {
        "privilege": "MANAGE GRANTS",
        "granted_on": "ACCOUNT",
    },
    {
        "privilege": "USAGE",
        "granted_on": "WAREHOUSE",
    },
]


def show_session_details(connection_name: str):
    snowflake_ds = SnowflakeDatasourceConnection(connection_name)

    res = snowflake_ds.execute(f"SHOW GRANTS TO USER {snowflake_ds.get_session().user}")
    user_roles = []
    if isinstance(res, list) and len(res) > 0:
        for i, grant in enumerate(res):
            user_roles.append(grant["role"])

    data = {
        "warehouse": snowflake_ds.get_session().warehouse,
        "user": snowflake_ds.get_session().user,
        "roles": user_roles,
    }
    print(data)
    snowflake_ds.close_session()


def show_missing_grants(connection_name: str):
    snowflake_ds = SnowflakeDatasourceConnection(connection_name)
    grants_to_user = snowflake_ds.execute(
        f"SHOW GRANTS TO USER {snowflake_ds.get_session().user}"
    )
    user_roles = []
    if isinstance(grants_to_user, list) and len(grants_to_user) > 0:
        for i, grant in enumerate(grants_to_user):
            user_roles.append(grant["role"])

    missing_grants = REQUIRED_GRANTS
    for role in user_roles:
        grants_to_role = snowflake_ds.execute(f"SHOW GRANTS TO ROLE {role}")
        if isinstance(grants_to_role, list) and len(grants_to_role) > 0:
            for i, grant in enumerate(grants_to_role):
                for required_grant in missing_grants:
                    if (
                        grant["privilege"] == required_grant["privilege"]
                        and grant["granted_on"] == required_grant["granted_on"]
                    ):
                        missing_grants.remove(required_grant)

    if len(missing_grants) == 0:
        print("User in session has all of the required grants")
    else:
        print(missing_grants)
    snowflake_ds.close_session()


def show_required_grants():
    print(REQUIRED_GRANTS)
