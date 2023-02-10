import logging
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os


class FileEventProducer:
    def __init__(self) -> None:
        log_handler = RotatingFileHandler(
            f'{os.path.abspath(os.getcwd())}/kafka/amz_consumer/logs/producer.log',
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('file_event_producer')

        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
            client_id='file_event_producer')

    def message_handler(self, message: str) -> None:
        #  Message from file event creator
        try:
            self.producer.send('fileEvent', bytes(message, encoding='utf-8'))
            self.producer.flush()
        except KafkaError as e:
            self.logger.error(f'An Kafka error happened: {e}')
        except Exception as e:
            self.logger.error(f'An error happened while pushing message to Kafka: {e}')
