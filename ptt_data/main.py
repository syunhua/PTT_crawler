import pandas as pd
from fastapi import FastAPI
from sqlalchemy import create_engine, engine


def get_mysql_pttdata_conn() -> engine.base.Connection:
    address = "mysql+pymysql://root:test@172.105.208.52:3306/pttdata"
    engine = create_engine(address)
    connect = engine.connect()
    return connect


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ptt")
def taiwan_stock_price(
    start_month,
    start_date,
    end_date,
):
    sql = f"""
    select * from ptt
    where LEFT(`Month_Days`,3)='{start_month}' 
    AND RIGHT(`Month_Days`,1)>'{start_date}'
    AND RIGHT(`Month_Days`,1)<'{end_date}'
    """
    mysql_conn = get_mysql_pttdata_conn()
    data_df = pd.read_sql(sql, con=mysql_conn)
    data_dict = data_df.to_dict("records")
    return {"data": data_dict}
