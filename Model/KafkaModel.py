import logging

from confluent_kafka import Producer, Consumer, KafkaException


class KafkaClient:
    """
    Kafka 客户端类
    """
    _instance = None  # 单例模式
    
    def __new__(cls, *args, **kwargs):
        # 如果 _instance 为 None，创建一个新的实例，否则返回已有的实例
        if not isinstance(cls._instance, cls):
            cls._instance = super(KafkaClient, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, bootstrap_servers):
        """
        初始化方法

        :param bootstrap_servers: Kafka 服务器地址
        """
        self.bootstrap_servers = bootstrap_servers
        if self._instance is None:
            self.producer = None
            self.consumer = None
    
    def create_producer(self):
        """
        创建 Kafka 生产者
        """
        try:
            self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})
        except KafkaException as e:
            logging.error(f"Failed to create Kafka producer: {e}")
            # 优雅地退出程序
            exit(1)
    
    def create_consumer(self, group_id, topics):
        """
        创建 Kafka 消费者

        :param group_id: 消费者组 ID
        :param topics: 要订阅的主题列表
        """
        try:
            self.consumer = Consumer({
                    'bootstrap.servers': self.bootstrap_servers,
                    'group.id': group_id,
                    'auto.offset.reset': 'earliest'
                
            })
            self.consumer.subscribe(topics)
        except KafkaException as e:
            logging.error(f"Failed to create Kafka consumer: {e}")
            # 优雅地退出程序
            exit(1)
    
    def send_message(self, topic, value):
        """
        发送消息

        :param topic: 要发送消息的主题
        :param value: 要发送的消息
        """
        if self.producer is None:
            print("Please create a producer before sending a message")
            return
        
        def delivery_report(err, msg):
            if err is not None:
                print('Message delivery failed: {}'.format(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        
        self.producer.produce(topic, value.encode('utf-8'), callback=delivery_report)
        self.producer.flush()
    
    def receive_messages(self):
        """
        接收消息
        """
        if self.consumer is None:
            print("Please create a consumer before receiving messages")
            return
        
        while True:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            
            print('Received message: {}'.format(msg.value().decode('utf-8')))
