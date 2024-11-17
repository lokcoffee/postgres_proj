import json
from codecs import ignore_errors

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
    import os

    # 设置需要遍历的文件夹路径
    folder_path = '../res/movies'
    datas = []
    # 遍历文件夹
    for dirname, subdirs, files in os.walk(folder_path):
        # print(f'Found directory: {dirname}')
        for file in files:
            print(f'{os.path.join(dirname, file)} is a file')
            with open(f'{os.path.join(dirname, file)}'.replace("\\", "/"), 'r', encoding="utf-8", errors="ignore") as json_file:
                data = json.load(json_file)
                datas.append(data)
    return datas

if __name__ == "__main__":
    conn = connect_to_db()
    query(conn, "SELECT version()")
    datas = load_json()
    for index, item in enumerate(datas):
        id = index + 1
        name = item["name"]
        year = item["year"]
        directors = {}
        if item.get("director"):
            if type(item["director"]) == list:
                directors = {index: ele for index, ele in enumerate(item["director"])}
            else:
                directors = {0: item["director"]}
        elif item.get("directors"):
            directors = {index: ele for index, ele in enumerate(item["directors"])}

        value = json.dumps(item)
        directors_value = json.dumps(directors)
        insert(conn, "INSERT INTO movies (id, name, year, directors, value) VALUES (%s, %s, %s, %s, %s)",
               (id, name, year, directors_value, value))
    conn.commit()
    conn.close()
