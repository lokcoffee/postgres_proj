import json

import psycopg2
import yaml

# 从Poetry的配置文件中获取数据库连接信息
# DATABASE_URL = "postgresql+psycopg2://username:password@localhost/database_name"
# DATABASE_URL = "postgresql+psycopg2://postgres:123456@192.168.56.101"


# 读取YAML配置文件

def load_yaml():
    with open("../res/database.yml", 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as ex:
            print(ex)


def connect_to_db():
    try:
        config = load_yaml()
        value = config["postgresql"]
        # 连接到数据库
        conn = psycopg2.connect(**value)
        return conn
    except psycopg2.Error as e:
        print("数据库连接失败:", e)


def query(conn, exec_sql):
    # 创建一个游标对象
    with conn.cursor() as cursor:
        # 执行一个查询
        cursor.execute(exec_sql)
        # 获取查询结果
        db_version = cursor.fetchone()
        print(db_version)


def insert(conn, upd_sql, data):
    with conn.cursor() as cursor:
        cursor.execute(upd_sql, data)


def load_json():
    with open("../res/carts.json", 'r') as json_file:
        data = json.load(json_file)
        return data

if __name__ == "__main__":
    conn = connect_to_db()
    query(conn, "SELECT version()")
    data = load_json()
    for item in data["carts"]:
        id = item["id"]
        products = item
        products = json.dumps(products)
        insert(conn, "INSERT INTO carts (id, products) VALUES (%s, %s)", (id, products))
    conn.commit()
    conn.close()
