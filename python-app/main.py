import pika
import logging
import colorlog
import time
import warnings

from crawler import Crawler
from cleaner import fetch_and_clean_html

# filter warnings
warnings.filterwarnings("ignore")

# setup logging
logger = colorlog.getLogger('NLPCrawl')
logger.setLevel(logging.DEBUG)
ch = colorlog.StreamHandler()
formatter_ch = colorlog.ColoredFormatter('%(log_color)s[%(name)s] %(message)s')
ch.setFormatter(formatter_ch)
logger.addHandler(ch)


# RabbitMQ settigns
RABBITMQ_HOST = 'rabbitmq'

# Queue Settings
HTML_QUEUE = 'HTML_QUEUE'
DOC_QUEUE = 'DOC_QUEUE'
FEATURE_QUEUE = 'FEAT_QUEUE'

# Thread Settings
NUM_CRAWL_THREAD = 8
NUM_PARSE_THREAD = 1
NUM_FEAT_THREAD = 1

# crawler settings
CRAWL_CONFIG = {
    'start_url': 'http://blog.schlerp.net',
    'include_urls': ['blog.schlerp.net'],
    'exclude_urls': [],
    'include_content': [],
    'exclude_content': [],
    'num_threads': NUM_CRAWL_THREAD,
    'html_queue': HTML_QUEUE, 
    'rabbitmq_host': RABBITMQ_HOST
}

# cleaner settings
CLEAN_CONFIG = {
    'rabbitmq_host': RABBITMQ_HOST,
    'html_queue': HTML_QUEUE,
    'doc_queue': DOC_QUEUE,
    'keep_numbers': True
}

# feature settings
FEATURE_CONFIG = {
    
}


def setup():
    '''sets up the rabbit MQ queues'''
    
    logger.info('├ Running setup()')
    logger.info('│ ├ Creating message queues...')

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            channel = connection.channel()
            # setup connection to rabbitmq
            logger.debug("│ ├ connected to rabbitmq at {}".format(RABBITMQ_HOST))
            break
        #except pika.exceptions.AMQPConnectionError as e:
        except Exception as e:
            time.sleep(3)
            pass

    # declare the channel for raw html
    logger.debug(f'│ ├ Creating html queue {HTML_QUEUE}')
    channel.queue_declare(queue=HTML_QUEUE)

    # declare the channel for cleaned docs
    logger.debug(f'│ ├ Creating docs queue {DOC_QUEUE}')
    channel.queue_declare(queue=DOC_QUEUE)

    # declare the channel for cleaned docs
    logger.debug(f'│ └ Creating features queue {FEATURE_QUEUE}')
    channel.queue_declare(queue=FEATURE_QUEUE)

    # close of the connection
    connection.close()


def main():
    '''calls the crawler, parser and cleaner scripts'''
    
    logger.info('├ Starting crawler!')
    wiki_crawler = Crawler(**CRAWL_CONFIG)
    wiki_crawler.crawl(max_pages=100)

    logger.info('├ Starting cleaner!')
    fetch_and_clean_html(**CLEAN_CONFIG)


if __name__ == '__main__':
    
    logger.info('Starting NLPCrawl!')

    setup()
    
    main()

    logger.info('└ Crawling complete!')