import psycopg2
from psycopg2 import extras, sql
import datetime
from typing import List
import databases
import sqlalchemy
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, TIMESTAMP, Float, func, select, desc
import uvicorn
from dateutils import relativedelta


PG_USER = "test_user_pg"
PG_PASSWORD = "testpassword"
PG_DBNAME = "DB_dz1"
PG_HOST = "127.0.0.1"
PG_PORT = "5432"

# подключение к базе, создание таблиц и наполнение с использованием pcycopg2 + SQL
PG_DSN = f"dbname={PG_DBNAME} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}"

conn = psycopg2.connect(PG_DSN, cursor_factory=extras.RealDictCursor)
with conn.cursor() as cursor:
    sql_query = """
        CREATE TABLE IF NOT EXISTS item
            ( id serial PRIMARY KEY,
              name VARCHAR NOT NULL UNIQUE,
              price float NOT NULL
            );


        CREATE TABLE IF NOT EXISTS store
            ( id serial PRIMARY KEY,
              address varchar NOT NULL UNIQUE
            );

        CREATE TABLE IF NOT EXISTS sales
            ( id serial PRIMARY KEY,
              sale_time timestamp NOT NULL,
              item_id INTEGER,
              store_id INTEGER,
              FOREIGN KEY (item_id) REFERENCES item (id),
              FOREIGN KEY (store_id) REFERENCES store (id)
            );
        """
    cursor.execute(sql_query)

    try:
        cursor.execute("insert into item (name, price) values ('Notepad Asus', 123000)")
        cursor.execute("insert into store (address) values ('Ярославль, Ярославская 9')")

        cursor.execute("insert into item (name, price) values ('Notepad Lenovo', 77000)")
        cursor.execute("insert into store (address) values ('Оренбург, Телекомовская 10')")

        cursor.execute("insert into item (name, price) values ('Notepad Dell', 98000)")
        cursor.execute("insert into store (address) values ('Краснодар, Телекомовская 10')")

        cursor.execute("insert into item (name, price) values ('Notebook Dell', 40000)")
        cursor.execute("insert into store (address) values ('Орел, Телекомовская 10')")
        cursor.execute("insert into item (name, price) values ('Notepad MSI', 50000)")
        cursor.execute("insert into store (address) values ('Курск, Телекомовская 10')")
        cursor.execute("insert into item (name, price) values ('Notepad Acer', 60000)")
        cursor.execute("insert into store (address) values ('Самара, Телекомовская 10')")
        cursor.execute("insert into item (name, price) values ('Notepad Apple', 150000)")
        cursor.execute("insert into store (address) values ('Уфа, Телекомовская 10')")
        cursor.execute("insert into item (name, price) values ('Notepad HP', 70000)")
        cursor.execute("insert into store (address) values ('Казань, Телекомовская 10')")
        cursor.execute("insert into item (name, price) values ('Notepad Huawei', 79999)")
        cursor.execute("insert into store (address) values ('Екатеринбург, Телекомовская 10')")
        cursor.execute("insert into item (name, price) values ('Notebook Hiper 222', 30000)")
        cursor.execute("insert into store (address) values ('Челябинск, Телекомовская 10')")
        cursor.execute("insert into item (name, price) values ('Notebook Microsoft', 89999)")
        cursor.execute("insert into store (address) values ('Курган, Телекомовская 10')")
        cursor.execute("insert into item (name, price) values ('Notebook Digma 333', 19999)")
        cursor.execute("insert into store (address) values ('Бугуруслан, Телекомовская 10')")

        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-01 15:15:15', (SELECT id FROM item where name='Notepad Asus' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Ярославль, Ярославская 9' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-02 15:15:15', (SELECT id FROM item where name='Notepad Lenovo' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Оренбург, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-03 15:15:15', (SELECT id FROM item where name='Notepad Dell' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Краснодар, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-04 15:15:15', (SELECT id FROM item where name='Notebook Dell' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Орел, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-05 15:15:15', (SELECT id FROM item where name='Notepad MSI' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Курск, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-06 15:15:15', (SELECT id FROM item where name='Notepad Acer' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Самара, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-07 15:15:15', (SELECT id FROM item where name='Notepad Apple' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Уфа, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-08 15:15:15', (SELECT id FROM item where name='Notepad HP' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Казань, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-09 15:15:15', (SELECT id FROM item where name='Notepad Huawei' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Екатеринбург, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-10 15:15:15', (SELECT id FROM item where name='Notebook Hiper 222' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Екатеринбург, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-11 15:15:15', (SELECT id FROM item where name='Notebook Microsoft' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Челябинск, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-12 15:15:15', (SELECT id FROM item where name='Notebook Digma 333' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Челябинск, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-13 15:15:15', (SELECT id FROM item where name='Notepad Asus' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Курган, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-14 15:15:15', (SELECT id FROM item where name='Notepad Lenovo' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Курган, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-15 15:15:15', (SELECT id FROM item where name='Notepad Dell' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Курган, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-02-16 15:15:15', (SELECT id FROM item where name='Notebook Dell' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Бугуруслан, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-31 15:15:15', (SELECT id FROM item where name='Notepad MSI' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Ярославль, Ярославская 9' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-30 15:15:15', (SELECT id FROM item where name='Notepad Acer' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Бугуруслан, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-31 15:15:15', (SELECT id FROM item where name='Notepad Apple' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Ярославль, Ярославская 9' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-31 15:15:15', (SELECT id FROM item where name='Notepad HP' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Бугуруслан, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-29 15:15:15', (SELECT id FROM item where name='Notepad Huawei' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Бугуруслан, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-22 15:15:15', (SELECT id FROM item where name='Notebook Hiper 222' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Уфа, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-20 10:15:15', (SELECT id FROM item where name='Notepad Asus' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Уфа, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-21 11:15:15', (SELECT id FROM item where name='Notepad Lenovo' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Оренбург, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-20 12:15:15', (SELECT id FROM item where name='Notepad Dell' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Ярославль, Ярославская 9' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-31 13:15:15', (SELECT id FROM item where name='Notebook Dell' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Оренбург, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
        cursor.execute("insert into sales (sale_time, item_id, store_id) values ('2023-01-31 14:15:15', (SELECT id FROM item where name='Notepad MSI' ORDER BY id DESC LIMIT 1), (SELECT id FROM store where address='Оренбург, Телекомовская 10' ORDER BY id DESC LIMIT 1))")
    except:
        print("БД уже наполнена данными по-умолчанию")

conn.commit()
conn.close()


# подключение к базе databases + эндпоинты на FastAPI + SQLalchemy + сервер uvicorn

DB_USER = "test_user_pg"
DB_NAME = "DB_dz1"
DB_PASSWORD = "testpassword"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

engine = sqlalchemy.create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


item = sqlalchemy.Table(
    "item",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("price", sqlalchemy.Float),
)

store = sqlalchemy.Table(
    "store",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("address", sqlalchemy.String),
)

sales = sqlalchemy.Table(
    "sales",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("sale_time", sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow()),
    sqlalchemy.Column("item_id", sqlalchemy.Integer, ForeignKey("item.id"), nullable=False),
    sqlalchemy.Column("store_id", sqlalchemy.Integer, ForeignKey("store.id"), nullable=False),
)

engine = sqlalchemy.create_engine(
    DATABASE_URL
)
metadata.create_all(engine)


class Item(BaseModel):
    id: int
    name: str
    price: float

class Store(BaseModel):
    id: int
    address: str

class Sales(BaseModel):
    id: int
    sale_time: datetime.datetime
    item_id: int
    store_id: int

class SalesIn(BaseModel):
    sale_time: datetime.datetime
    item_id: int
    store_id: int

app = FastAPI()

@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/items_all/", response_model=List[Item])
async def read_all_item():
    query = item.select()
    return await database.fetch_all(query)

@app.get("/stores_all/", response_model=List[Store])
async def read_all_store():
    query = store.select()
    return await database.fetch_all(query)

@app.post("/sales_post/", response_model=Sales)
async def create_sales(sale: SalesIn):
    query = sales.insert().values(sale_time=datetime.datetime.utcnow(), item_id=sale.item_id, store_id=sale.store_id)
    last_record_id = await database.execute(query)
    #print(last_record_id)
    return {**sale.dict(), "id": last_record_id}

@app.get("/TOP10_store/")
async def create_top10_store():
    #переменная месяц назад
    sales_month = datetime.datetime.now().date() - relativedelta(month=1)

    # SQLalchemy-запрос на получение данных по топ 10 самых доходных магазинов
    # за месяц (id + адрес + суммарная выручка)
    query_top_store = select(store.c.id, store.c.address, func.sum(item.c.price).label("Summarnaya_vyruchka"))\
        .where(store.c.id == sales.c.store_id, item.c.id == sales.c.item_id, sales.c.sale_time > sales_month)\
        .group_by(store.c.id)\
        .order_by(desc("Summarnaya_vyruchka"))\
        .limit(10)
    return await database.fetch_all(query_top_store)

@app.get("/TOP10_item/")
async def create_top10_item():
    # обрабатывает GET-запрос на получение данных по топ 10 самых
    # продаваемых товаров (id + наименование + количество проданных товаров)
    query_top_item = select(item.c.id, item.c.name, func.count(sales.c.id).label("Количество_проданных_товаров"))\
        .where(item.c.id == sales.c.item_id)\
        .group_by(item.c.id)\
        .order_by(desc("Количество_проданных_товаров"))\
        .limit(10)
    return await database.fetch_all(query_top_item)

uvicorn.run(app, host="127.0.0.1", port=8088, log_level="info")

