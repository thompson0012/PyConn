from sqlalchemy import inspect
from pydb.ops.db.base import DBOperator
from pydb.enum.db_layer import DBLayer
from addict import Addict


class TableGateKeeper(DBOperator):
    def __init__(self, engine):
        super(TableGateKeeper, self).__init__(engine)

    @staticmethod
    def standardize_tbl_name(db_layer: str, department: str, business: str, table_name: str,
                             update_period: str):
        TABLE_SEP = '_'
        table_struct = [db_layer, department, business, table_name, update_period]
        small_cap_table_struct = [i.lower() for i in table_struct]
        full_table_name = TABLE_SEP.join(small_cap_table_struct)

        return full_table_name

    def standardize_idx_name(self, table_name: str):
        return '_'.join(['ix', table_name])
