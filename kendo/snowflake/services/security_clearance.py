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

REQUIRED_ROLES = [
    "SYSADMIN",
    "SECURITYADMIN",
]


def show_session_details(connection_name: str):
    session = get_session(connection_name)

    res = execute(session, f"SHOW GRANTS TO USER {session.user}")
    user_roles = []
    for i, grant in enumerate(res):
        user_roles.append(grant["role"])

    data = {
        "warehouse": session.warehouse,
        "user": session.user,
        "roles": user_roles,
    }
    print(data)
    close_session(session)


def show_missing_grants(connection_name: str):
    session = get_session(connection_name)
    grants_to_user = execute(session, f"SHOW GRANTS TO USER {session.user}")
    user_roles = []
    for i, grant in enumerate(grants_to_user):
        user_roles.append(grant["role"])

    missing_grants = REQUIRED_GRANTS
    for role in user_roles:
        grants_to_role = execute(session, f"SHOW GRANTS TO ROLE {role}")
        assert grants_to_role is not None
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
    close_session(session)


def show_required_roles():
    print(REQUIRED_ROLES)
