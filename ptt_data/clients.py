from sqlalchemy import (
    create_engine,
    engine,
)


def get_mysql_pttdata_conn() -> engine.base.Connection:
    """
    user: root
    password: test
    host: localhost
    port: 3306
    database: financialdata
    如果有實體 IP，以上設定可以自行更改
    """
    address = "mysql+pymysql://root:test@172.105.208.52:3306/pttData"
    engine = create_engine(address)
    connect = engine.connect()
    return connect
