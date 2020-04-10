import requests
from bs4 import BeautifulSoup
import re
import boto3
import config
import json
sqs = boto3.client('sqs')
from sys import argv
def striphtml(data):
    p = re.compile(r'<.*?>')
    return p.sub('', data)


def news_content_crawler(url):
    try:
        print("news crawler")
        news = requests.get(url)
        encodedText = (news.text).encode('utf8')
        soup = BeautifulSoup(encodedText, 'html.parser')
        content = soup.find(class_='story-body')
        paragraph = content.find_all('p')
        print(type(paragraph))
        return striphtml(str(paragraph))  # striphtml(paragraph)
    #   push_to_sqs(paragraph)
    except Exception as e:
        print("Error while getting news", e)
        return None


def upload_in_s3(filename, bucketname):
    s3 = boto3.resource('s3')
    file_loc = '/tmp/' + filename
    s3.meta.client.upload_file(file_loc, bucketname, filename)
    message_id = send_message(
        {"source": "twitter", "file": filename, "text": None})
    print(message_id)

def send_message(message_data):
        
        queue_url = config.SQS_URL
        # Send message to SQS queue
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=10,
            MessageAttributes={
                'Title': {
                    'DataType': 'String',
                    'StringValue': 'Sentiment Analysis'
                },
                'Author': {
                    'DataType': 'String',
                    'StringValue': 'Dev Team'
                },
                'WeeksOn': {
                    'DataType': 'Number',
                    'StringValue': '6'
                }
            },
            MessageBody=(json.dumps(message_data))
        )

        print(response['MessageId'])
        return response['MessageId']
    



def get_articles(source, topic):
    url = "https://newsapi.org/v2/everything?q={}&apiKey=a63bb6817c46475d885559d30e8a82cd&from=2019-03-13&to=2019-04-05&sources=bbc-news&pageSize=100&sortBy=publishedAt&language=en".format(topic)
    r = requests.get(url)
    print(r.json())
    data = r.json()
    output = []
    for article in data['articles']:
        article['content'] = news_content_crawler(article['url'])
        output.append(json.dumps(article))
    print(output)
    filename = source + topic + '20191204.json'
    print(filename)
    with open('/tmp/' + filename, 'w') as outfile:
        outfile.write('\n'.join(output))
    upload_in_s3(filename, config.S3_BUCKET)


source, topic = argv[1], argv[2]
get_articles(topic, source)
