import colorlog
import pika
import requests
from bs4 import BeautifulSoup
import threading
import time
from multiprocessing.pool import ThreadPool
from urllib.parse import urlparse, urljoin

logger = colorlog.getLogger('NLPCrawl')

def store_html(url, html, queue, rabbitmq_host):
    logger.info("│ │ ├ Storing HTML from {}...".format(url))

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
            channel = connection.channel()
            logger.debug("│ │ │ ├ connected to rabbitmq at {}".format(rabbitmq_host))
            break
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning("│ │ │ ├ {}".format(e))
            time.sleep(3)
            pass
    
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=html
    )
    logger.debug("│ │ │ └ HTML published to rabbitmq!")


class Crawler(object):
    def __init__(self, start_url, 
                 include_urls, exclude_urls, 
                 include_content, exclude_content, 
                 num_threads, html_queue, rabbitmq_host,
                 empty_wait=3, empty_retry_count=10):
        '''Initialise the crawler and setup variables'''
        self.start_url = start_url
        self.include_urls = include_urls
        self.exclude_urls = exclude_urls
        self.include_content = include_content
        self.exclude_content = exclude_content
        self.num_threads = num_threads
        self.html_queue = html_queue
        self.rabbitmq_host = rabbitmq_host
        self.empty_wait = empty_wait
        self.empty_retry_count = empty_retry_count

        self._urls_to_crawl = set([start_url,])
        self._urls_to_crawl_lock = threading.Lock()
        self._urls_been_crawled = set()
        self._urls_been_crawled_lock = threading.Lock()

    def crawl(self, max_pages=None):
        '''start the crawler in motion'''
        crawl_pool = ThreadPool(self.num_threads)

        logger.info('│ ├ Starting {} crawler threads...'.format(self.num_threads))
        
        crawl_pool.apply(self._crawl_thread, args=(self.start_url,))

        empty_count = 0
        while True:
            crawl_pool.apply_async(self._crawl_thread)

            # if queue is empty sleep
            if len(self._urls_to_crawl) == 0:
                empty_count += 1
                time.sleep(self.empty_wait)

            # if queue is still empty after sleeping/retrying x many times then exit
            if empty_count >= self.empty_retry_count:
                break
        
        crawl_pool.close()
        crawl_pool.join()
        
        logger.info('│ └ crawled {} links!'.format(len(self._urls_been_crawled)))
    
    def _crawl_thread(self, url=None):
        '''internal method to crawl a page and extract information 
        using excludes/includes. this method is 1 thread of crawler'''
        if not url:
            with self._urls_to_crawl_lock:
                if len(self._urls_to_crawl) > 0:
                    url = self._urls_to_crawl.pop()
                else:
                    return
        
        if url:
            logger.info('│ │ ├ Fetching {}...'.format(url))
            result = requests.Session()
            result = result.get(url, timeout=5, verify=False)
            logger.info('│ │ │ └ result code {}...'.format(result.status_code))

            if result.status_code == 200:
                with self._urls_been_crawled_lock:
                    self._urls_been_crawled.add(url)
                c = result.text
                soup = BeautifulSoup(c)
                links = soup.findAll('a')
                for a in links:
                    # logger.debug('│ │ ├ checking link: {}'.format(a))
                    try:
                        a = a.attrs['href']
                        parsed_url = urlparse(url)
                        if parsed_url.scheme == '': 
                            parsed_url = parsed_url._replace(scheme='http')
                        base_url = parsed_url[0] + '://' + parsed_url[1]
                        a = urljoin(base_url, a)
                        # logger.debug('│ │ ├ expanded link: {}'.format(a))
                    except:
                        continue
                    if self._filter_link(a):
                        with self._urls_been_crawled_lock:
                            if a not in self._urls_been_crawled:
                                # logger.debug('│ │ ├ storing link: {}'.format(a))
                                with self._urls_to_crawl_lock:
                                    self._urls_to_crawl.add(a)
                
                if self._filter_content(c):
                    store_html(url, c, self.html_queue, self.rabbitmq_host)
                    return True
        logger.info('│ │ │ └ Excluded HTML!...')
        return False

    def _filter_link(self, a):
        '''applies url filtering rules to link'''
        for inc_url in self.include_urls:
            if inc_url in a:
                return True
        return False
    
    def _filter_content(self, c):
        '''applies content filtering rules to content'''
        return True



def crawler(config):
    # breakout config values
    start_url = config['start_url']
    include_urls = config['include_urls']
    exclude_urls = config['exclude_urls']
    include_strings = config['include_strings']
    exclude_strings = config['exclude_strings']
    num_threads = config['num_threads']

    logger.info('├ Crawler created!')
    
    

    r = requests.get(start_url)