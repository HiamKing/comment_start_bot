import logging
import os
from logging.handlers import RotatingFileHandler
from kafka import KafkaConsumer
from hdfs import InsecureClient


class FileEventConsumer:
    def __init__(self) -> None:
        log_handler = RotatingFileHandler(
            f'{os.path.abspath(os.getcwd())}/kafka/amz_consumer/logs/consumer.log',
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('file_event_consumer')
        self.consumer = KafkaConsumer(
            'fileEvent',
            bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
            group_id='fileEventConsummers',
            auto_offset_reset='earliest',
            enable_auto_commit=False)
        self.hdfs_client = InsecureClient('http://localhost:9870', user='root')

    def run(self) -> None:
        try:
            self.logger.info('Subcribe to topic fileEvent')
            while True:
                msgs_pack = self.consumer.poll(10)
                if msgs_pack is None:
                    continue

                for tp, messages in msgs_pack.items():
                    for message in messages:
                        decode_msg = str(message[6], 'utf-8')
                        print(decode_msg)

        except Exception as e:
            self.logger.error(
                f'An error happened while processing messages from kafka: {e}')
        finally:
            self.consumer.close()


a = FileEventConsumer()

a.run()
