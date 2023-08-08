import logging

import psycopg2
from psycopg2 import sql
from psycopg2 import pool


class PostgresClient:
    
    def __init__(self, config):
        """
        初始化方法

        :param config，读取config.json文件形成一个ThreadedConnectionPool的参数
        """
        self.conn_pool = psycopg2.pool.ThreadedConnectionPool(**config)
    
    def flatten_dict(self, d, parent_key='', sep='_'):
        """
        展平嵌套的字典

        :param d: 需要展平的字典
        :param parent_key: 父键
        :param sep: 父键和子键之间的分隔符
        :return: 展平后的字典
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    def insert_data(self, schema, table, data, convert_to_lower=True):
        """
        将数据插入到 PostgreSQL
        :param schema: 模式名
        :param table: 表名
        :param data: 需要插入的数据，是一部字典，键是列名，值是对应的数据
        :param convert_to_lower: 是否将列名转换为小写
        """
        conn = self.conn_pool.getconn()
        
        # 展平嵌套的字典
        flat_data = self.flatten_dict(data)
        
        # 如果 convert_to_lower 为真，则将所有的键转换为小写
        if convert_to_lower:
            flat_data = {k.lower(): v for k, v in flat_data.items()}
        
        try:
            with conn.cursor() as cur:
                # 创建 SQL 插入语句
                insert = sql.SQL("INSERT INTO {schema}.{table} ({cols}) VALUES ({vals})").format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(table),
                        cols=sql.SQL(',').join(map(sql.Identifier, flat_data.keys())),
                        vals=sql.SQL(',').join(map(sql.Placeholder, flat_data.keys()))
                )
                # 执行 SQL 插入语句
                cur.execute(insert, flat_data)
                conn.commit()
        except Exception as e:
            logging.error("数据库插入逻辑错误：%s", e)
        finally:
            self.conn_pool.putconn(conn)  # 将连接返回到连接池
