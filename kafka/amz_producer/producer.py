import logging
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os


class AmazonProducer:
    def __init__(self) -> None:
        log_handler = RotatingFileHandler(
            f'{os.path.abspath(os.getcwd())}/kafka/amz_producer/logs/producer.log',
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('amazon_producer')

        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
            client_id='amazon_producer')

    def message_handler(self, message: dict) -> None:
        #  Message from amazon comments crawler
        try:
            if (len(message.keys()) == 3):
                amazon_cmt_data = f"{message['title']}[this_is_sep]{message['rating']}[this_is_sep]{message['content']}"
                self.producer.send('amazonCmtData', bytes(amazon_cmt_data, encoding='utf-8'))
                self.producer.flush()
        except KafkaError as e:
            self.logger.error(f'An Kafka error happened: {e}')
        except Exception as e:
            self.logger.error(f'An error happened while pushing message to Kafka: {e}')