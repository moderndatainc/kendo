from rich import print
from kendo.snowflake.connection import get_session, close_session, execute


def show_session_details(connection_name: str):
    session = get_session(connection_name)
    data = {
        "warehouse": session.warehouse,
        "user": session.user,
        "role": session.role,
    }
    print(data)
    close_session(session)


def show_current_role_grants(connection_name: str):
    session = get_session(connection_name)
    res = execute(session, f"SHOW GRANTS TO ROLE {session.role}")
    res = list(
        map(
            lambda grant: {
                "privilege": grant["privilege"],  # type: ignore
                "granted_on": grant["granted_on"],  # type: ignore
                "name": grant["name"],  # type: ignore
                "grant_option": grant["grant_option"],  # type: ignore
            },
            res,  # type: ignore
        )
    )
    print(res)
    close_session(session)


def show_required_grants():
    data = [
        "CREATE DATABASE ON ACCOUNT",
        "CREATE ROLE ON ACCOUNT",
        "MANAGE GRANTS ON ACCOUNT",
    ]
    print(data)
