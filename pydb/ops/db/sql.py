from sqlglot.optimizer import optimize
import sqlglot


class SQLOperator:
    def __init__(self):
        pass

    def transpile(self, sql, read, write):
        return sqlglot.transpile(sql, read, write)

    def optimize(self, ):
