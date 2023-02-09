from consumer import AmazonConsumer


def run_services():
    crawler = AmazonConsumer()
    crawler.run()


run_services()
