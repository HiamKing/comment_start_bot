import os
import logging
from typing import List
from requests_html import HTMLSession, Element
from logging.handlers import RotatingFileHandler
from producer import AmazonProducer
import threading

log_handler = RotatingFileHandler(
    f'{os.path.abspath(os.getcwd())}/kafka/amz_producer/logs/scraper.log',
    maxBytes=104857600, backupCount=10)
logging.basicConfig(
    format='%(asctime)s <%(name)s>[%(levelname)s]: %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S',
    level=logging.INFO,
    handlers=[log_handler])
logger = logging.getLogger('amazon_scraper')

USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'

amz_producer = AmazonProducer()


class ReviewScraper:
    def __init__(self, category: str, asin: str, price: float) -> None:
        self.category = category
        self.asin = asin
        self.price = price
        self.url = f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_getr_d_paging_btm_prev_1?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber='
        self.session = HTMLSession()
        self.headers = {'User-Agent': USER_AGENT}

    def paginate(self, page: int) -> List[Element]:
        retry = 0
        raw_html = []
        while not raw_html:
            response = self.session.get(self.url + str(page))
            raw_html = response.html.find('div[data-hook=review]')
            retry += 1
            # retry 10 times
            if retry > 10 or raw_html:
                break
        return raw_html

    def get_review_title(self, item: Element) -> str:
        title = item.find('a[data-hook=review-title]', first=True)
        if not title:
            title = item.find('span[data-hook=review-title] span', first=True)

        return title.text

    def get_review_rating(self, item: Element) -> float:
        rating = item.find('i[data-hook=review-star-rating] span', first=True)
        if not rating:
            rating = item.find('i[data-hook=cmps-review-star-rating] span', first=True)

        return float(rating.text.replace(' out of 5 stars', ''))

    def parse_n_send_raw_data(self, data: List[Element]) -> None:
        for item in data:
            try:
                review = {
                    'category': self.category,
                    'asin': self.asin,
                    'price': self.price,
                    'title': self.get_review_title(item),
                    'rating': self.get_review_rating(item),
                }
                contents = item.find('span[data-hook=review-body] span')
                # for remove images, video tag
                for content in contents:
                    if 'class' not in content.attrs:
                        review['content'] = content.text
                        break
                if review['title'] and 'content' in review and review['content']:
                    amz_producer.message_handler(review)
            except Exception as e:
                logger.error(f'An error happened when parsing raw data f{e}')

    def start_crawling_reviews(self) -> None:
        page = 1
        logger.info(f'Start crawling reviews of product {self.asin}')
        while True:
            logger.info(f'Start crawling reviews in page {page}')
            raw_data = self.paginate(page)
            if not raw_data:
                logger.info(f'No more reviews of product {self.asin}')
                break

            self.parse_n_send_raw_data(raw_data)
            page += 1
            if page > 100:
                break

        logger.info(f'End crawling reviews of product {self.asin}')


class ProductScraper:
    def __init__(self, category: str) -> None:
        self.category = category
        self.url = f'https://www.amazon.com/s?i={category}&page='
        self.session = HTMLSession()
        self.headers = {'User-Agent': USER_AGENT}

    def paginate(self, page) -> List[Element]:
        retry = 0
        raw_html = []
        while not raw_html:
            response = self.session.get(self.url + str(page))
            raw_html = response.html.find('div[data-asin]')
            retry += 1
            # retry 10 times
            if retry > 100 or raw_html:
                break

        result_product = []
        for data in raw_html:
            if data.attrs['data-asin'] != '':
                price = data.find('span[class=a-price] span', first=True)
                if price:
                    price = price.text.replace('$', '')
                result_product.append({
                    'asin': data.attrs['data-asin'],
                    'price': price
                })
        return result_product


class CategoryScraper(threading.Thread):
    def __init__(self, category: str) -> None:
        threading.Thread.__init__(self)
        self.category = category
        self.product_scraper = ProductScraper(category)

    def run(self) -> None:
        logger.info(f'Start crawling reviews of category {self.category}')
        page = 2
        while True:
            try:
                logger.info(f'Start crawling product asins in page {page}')
                product_list = self.product_scraper.paginate(page)

                for product in product_list:
                    review_scraper = ReviewScraper(self.category, product['asin'], product['price'])
                    review_scraper.start_crawling_reviews()
            except Exception as e:
                logger.error(f'An error happened when crawling product asins in page {page}: {e}')
            page += 1
            if page > 200:
                logger.info(f'No more products of category {self.category}')
                break

        logger.info(f'End crawling reviews of category {self.category}')


class AmazonScraper:
    def run(self) -> None:
        with open(f'{os.path.abspath(os.getcwd())}/kafka/amz_producer/categories.txt', 'r') as categories:
            threads = []
            for category in categories:
                thread = CategoryScraper(category.replace('\n', ''))
                thread.start()
                threads.append(thread)
            for t in threads:
                t.join()
