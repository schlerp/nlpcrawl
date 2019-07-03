from bs4 import BeautifulSoup
import unicodedata
import re
import nltk
import pika
import colorlog

from stopwords import stop_words

logger = colorlog.getLogger('NLPCrawl')

# inspired by:
# https://www.kdnuggets.com/2018/08/practitioners-guide-processing-understanding-text-2.html


def fetch_and_clean_html(rabbitmq_host, html_queue, doc_queue, keep_numbers=True):
    logger.info("│ │ ├ Fetching HTML from RabbitMQ...")

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
            channel_consume = connection.channel()
            channel_publish = connection.channel()
            logger.debug("│ │ │ ├ connected to rabbitmq at {}".format(rabbitmq_host))
            break
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning("│ │ │ ├ {}".format(e))
            time.sleep(3)
            pass
    
    for method, properties, body in channel_consume.consume(html_queue, ):
        # logger.debug('│ │ │ ├ cleaning "{}"...'.format(body[0:25]))
        text = clean_html(body, keep_numbers)
        logger.debug('│ │ │ ├ cleaned "{}"...'.format(text[0:25]))
        channel_publish.basic_publish(
            exchange='',
            routing_key=doc_queue,
            body=text
        )
        logger.debug("│ │ │ └ Cleaned doc published to rabbitmq!")
        channel_consume.basic_ack(method.delivery_tag)

        # check if queue is empty, if so exit
        if channel_consume.get_waiting_message_count() == 0:
            channel_consume.cancel()


def html_to_text(html):
    '''extracts the text from HTML formatted data'''
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()

def text_to_ascii(text):
    '''normalises text to be ascii only chars as utf8'''
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore')
    return text.decode('utf-8', 'ignore')

def strip_punctuation(text):
    '''removes punctuation from the text'''
    return re.sub(r'[^a-zA-z0-9\s]', '', text)

def strip_numbers(text):
    '''removes numbers rom the text'''
    return re.sub(r'[^a-zA-z\s]', '', text)

def text_to_lowercase(text):
    '''converts text to be all lower case'''
    return text.lower()

def stem_text(text, stemmer=nltk.stem.SnowballStemmer("english")):
    '''stems words to create more robust training data'''
    return ' '.join([stemmer.stem(word) for word in text.split()])

def tokenize_text(text):
    '''tokenises the text (creates a list of words)'''
    tokens = nltk.tokenize.toktok.ToktokTokenizer().tokenize(text)
    return [token.strip() for token in tokens]

def clean_html(html, keep_numbers=True):
    text = html_to_text(html)
    text = text_to_ascii(text)
    text = strip_punctuation(text)
    if not keep_numbers:
        text = strip_numbers(text)
    text = text_to_lowercase(text)
    text = stem_text(text)
    return text