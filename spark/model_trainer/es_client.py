from elasticsearch import Elasticsearch

ES_HOST = 'http://elasticsearch:9200'


class ESClient:
    def __init__(self) -> None:
        self.client = Elasticsearch(ES_HOST)

    def send_train_data(self) -> None:
        pass

    def send_predict_data(self) -> None:
        pass

    def send_category_data(self) -> None:
        pass

    def send_id_data(self) -> None:
        pass

    def send_price_data(self) -> None:

a = ESClient()

a.send_train_data()