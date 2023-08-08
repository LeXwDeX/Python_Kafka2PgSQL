import json
import logging
from Data.TopUp import TopUp
from Model.Logger import AsyncLogger
from Model.PostgresModel import PostgresClient
from Model.KafkaModel import KafkaClient
from Model.DingTalk import DingTalkNotifier
from Controller.Processor import Processor

logger = AsyncLogger('./log/kafka2pgsql.log')

# 从配置文件中读取配置信息
with open("config.json", "r") as file:
    config = json.load(file)

# 使用配置信息创建 PostgreSQL 客户端 Kafka 客户端实例 和 钉钉通知实例
pg = PostgresClient(config['db_config'])
ka = KafkaClient(bootstrap_servers=config['kafka_config']['bootstrap.servers'])
dt = DingTalkNotifier(config['dingtalk_config'])


def topup():  # <- 修改对应的类名称
    ka.create_consumer(group_id='16', topics=['topup'])  # <- 修改对应的消费者ID和主题名称
    p = Processor(ka, pg, TopUp(), schema=config['table_config']['schema'], table=config['table_config']['table_name'])  # <- 修改对应的模型名称(TopUp())
    while True:
        message = ka.consumer.poll()  # 消费者实例获取业务，拉取每个订阅的主题/分区的最新消息。这是一个阻塞的方法：如果没有可用的数据，它将等待直到有数据可用。
        if message is None:
            continue
        if message.error():
            dt.send_message("Consumer Message error： {}".format(message.error()))
            print("Consumer Message error： {}".format(message.error()))
            logger.log(logging.ERROR, ("Consumer Message error： {}".format(message.error())))
            continue
        msg = message.value()
        try:
            p.process_data(msg, convert_to_lower=True)  # convert_to_lower 默认把JSON的KEY映射成表列名，并转为小写
        except Exception as e:
            dt.send_message(f"Consumer Exception error：, {e}, {msg}")
            print(f"Consumer Exception error：, {e}, {msg}")
            logger.log(logging.ERROR, f"Consumer Exception error：, {e}, {msg}")


# 你可以像上面那样，为你的其他数据模型（如TopUp、CreateAcct和Login）添加相应的函数

if __name__ == '__main__':
    from concurrent.futures import ThreadPoolExecutor
    
    
    def safe_func(func):
        """包装函数以捕获异常"""
        
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                dt.send_message(f"Function Exception {func.__name__} raised {e}")
                print(f"Function Exception {func.__name__} raised {e}")
                logger.log(logging.ERROR, f"Function Exception {func.__name__} raised {e}")
        
        return wrapper
    
    
    def concurrent_executor():
        # 用列表存储所有函数
        funcs = [
                safe_func(topup),
                # 在这里添加你的其他函数
        ]
        # 创建一个ThreadPoolExecutor实例，max_workers参数表示工作线程的最大数量，推荐有几个函数就几个线程。（注意！！KAFAKA是单例模式，只允许从一个KAFAKA中获取数据！）
        with ThreadPoolExecutor(max_workers=1) as executor:
            results = executor.map(lambda func: func(), funcs)
        return list(results)
    
    
    concurrent_executor()
