import logging
import time
from psycopg2 import Error as PostgresError


class Processor:
    def __init__(self, kafka_client, postgres_client, data_model, schema, table):
        """
        初始化数据处理器

        :param kafka_client: Kafka 客户端实例
        :param postgres_client: PostgreSQL 客户端实例
        :param data_model: 数据模型实例
        :param table: PostgreSQL 数据库表名
        """
        self.kafka_client = kafka_client
        self.postgres_client = postgres_client
        self.data_model = data_model
        self.schema = schema
        self.table = table
        self.logger = logging.getLogger('Processor')  # 初始化日志
    
    def parse_and_validate(self, msg):
        """
        解析并验证数据
        :param msg: 从 Kafka 获取的消息
        :return: 解析后的数据或者None（如果数据无效）
        """
        try:
            parsed_msg = msg.decode('utf-8')
            return self.data_model.validate_data(parsed_msg)
        except Exception as e:
            self.logger.error(f'解码msg信息失败：{e}')
    
    def save_data_to_db(self, data, convert_to_lower, max_retry=3, ):
        """
        将数据保存到 PostgreSQL
        :param data: 要保存的数据
        :param max_retry: 最大重试次数
        :param convert_to_lower： 是否小写
        """
        for i in range(max_retry):  # 重试机制
            try:
                self.postgres_client.insert_data(self.schema, self.table, data, convert_to_lower)
                self.logger.info("数据已写入 PostgreSQL。")  # 使用日志记录信息
                return
            except PostgresError as e:
                self.logger.error(f"写入数据到 PostgreSQL 时出错：{e}")
                time.sleep(2 ** i)  # 指数退避
        else:  # 所有重试都失败
            self.logger.error("达到最大重试次数后，仍未能将数据写入 PostgreSQL。")
    
    def process_data(self, msg, convert_to_lower, max_retry=3):
        """
        处理数据
        :param msg: 从 Kafka 获取的消息
        :param max_retry: 最大重试次数
        :param convert_to_lower：默认把列名转为小写
        """
        parsed_data = self.parse_and_validate(msg=msg)
        if parsed_data is not None:
            self.save_data_to_db(parsed_data, max_retry, convert_to_lower)
        else:
            self.logger.warning("数据验证失败，跳过此消息。")  # 如果数据验证失败，记录一条警告日志并直接返回，跳过当前消息
