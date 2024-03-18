from rich import print
from kendo.snowflake.connection import get_session, close_session, execute

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
    session = get_session(connection_name)
    data = {
        "warehouse": session.warehouse,
        "user": session.user,
        "role": session.role,
    }
    print(data)
    close_session(session)


def show_missing_grants(connection_name: str):
    session = get_session(connection_name)
    res = execute(session, f"SHOW GRANTS TO ROLE {session.role}")
    assert res is not None
    missing_grants = REQUIRED_GRANTS
    for i, grant in enumerate(res):
        for required_grant in missing_grants:
            if (
                grant["privilege"] == required_grant["privilege"]
                and grant["granted_on"] == required_grant["granted_on"]
            ):
                missing_grants.remove(required_grant)

    if len(missing_grants) == 0:
        print("Role in session has all of the required grants")
    else:
        print(missing_grants)
    close_session(session)


def show_required_grants():
    print(REQUIRED_GRANTS)


def permit_account_usage_views(connection_name: str):
    session = get_session(connection_name)

    close_session(session)
