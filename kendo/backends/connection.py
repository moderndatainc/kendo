from abc import ABC, abstractmethod
from typing import Any


class IBackendConnection(ABC):
    session: Any

    @abstractmethod
    def get_session(self):
        pass

    @abstractmethod
    def execute(
        self,
        sql,
        sql_params=None,
        print_sql=False,
        abort_on_exception=True,
    ):
        pass

    @abstractmethod
    def execute_many_times(
        self,
        sql,
        list_of_sql_params=None,
        print_sql=False,
        abort_on_exception=True,
    ):
        pass

    @abstractmethod
    def execute_multi_stmts(
        self,
        sql,
        print_sql=False,
        abort_on_exception=True,
    ):
        pass

    @abstractmethod
    def close_session(self):
        pass
