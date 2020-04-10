from sys import argv
def lambda_handler(source, filename):
    import os
    import json
    import config
    from googletrans import Translator
    translator = Translator()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './GoogleCloudAccessKey.json'

    count =1
    if source == 'twitter':
        print("twitter")
        output_data = []
        context_data = read_file_form_s3(config.S3_BUCKET, filename)
        try:
            for data in context_data:
                data = json.loads(data)
                try:
                    text = data["retweeted_status"]["extended_tweet"]["full_text"] 
                    try:
                        text = translator.translate(text).text
                    except Exception as e:
                        print("language not supporting.",e)
                    data['analysis'] = {}
                    
                    data['analysis']['sentiment'] = json.loads(
                        sentiment_text(text))
                    data['analysis']['classifiction'] = json.loads(
                        classify_text(text))
                    output_data.append(json.dumps(data))
                    print("______________{}__________: {}".format(filename, count))
                    count +=1
                except Exception as e:
                    print(e)
            filename = 'analysed_' + filename.split('/')[-1]
            with open(filename, 'w') as outfile:
                outfile.write('\n'.join(output_data))
            upload_in_s3(filename, config.S3_BUCKET, output_data)
            print("file uploaded successfully ")
        except Exception as e:
            print(e)
    return None

def upload_in_s3(filename, bucketname, data):
    import boto3
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(filename, bucketname, 'analysed-report-brexit-sdgs/'+filename)

def read_file_form_s3(bucket, filename):
    print("inside read_file_form_s3")
    print(filename)
    import boto3
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, filename)
    data = obj.get()['Body'].read().decode('utf-8') 
    return data.split('\n')

def send_message(message_data):
        import json
        import boto3
        import config
        sqs = boto3.client('sqs')
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

def sentiment_text(text):
    # [START language_sentiment_text]
    # print("*****************Sentiment Analysis*****************")
    from google.cloud import language
    from google.cloud.language import enums
    from google.cloud.language import types
    from google.protobuf.json_format import MessageToJson

    client = language.LanguageServiceClient()

    try:
        text = text.decode('utf-8')
    except AttributeError:
        pass

    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    sentiment = MessageToJson(client.analyze_sentiment(document))
    # print(sentiment)
    return sentiment


def entities_text(text):
    print("*****************Entities Analysis*****************")
    import six
    from google.cloud import language
    from google.cloud.language import enums
    from google.cloud.language import types
    from google.protobuf.json_format import MessageToJson

    client = language.LanguageServiceClient()

    if isinstance(text, six.binary_type):
        text = text.decode('utf-8')

    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)
    entities = client.analyze_entities(document)
    return MessageToJson(entities)


def syntax_text(text):
    # [START language_syntax_text]
    print("*****************Syntax Analysis*****************")
    import six
    from google.cloud import language
    from google.cloud.language import enums
    from google.cloud.language import types
    from google.protobuf.json_format import MessageToJson

    client = language.LanguageServiceClient()

    if isinstance(text, six.binary_type):
        text = text.decode('utf-8')
    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    tokens = MessageToJson(client.analyze_syntax(document))
    return tokens


def entity_sentiment_text(text):
    # [START language_entity_sentiment_text]
    print("*****************Entity Sentiment Analysis*****************")
    import six
    import sys
    from google.cloud import language
    from google.cloud.language import enums
    from google.cloud.language import types
    from google.protobuf.json_format import MessageToJson

    client = language.LanguageServiceClient()

    if isinstance(text, six.binary_type):
        text = text.decode('utf-8')

    document = types.Document(
        content=text.encode('utf-8'),
        type=enums.Document.Type.PLAIN_TEXT)

    # Detect and send native Python encoding to receive correct word offsets.
    encoding = enums.EncodingType.UTF32
    if sys.maxunicode == 65535:
        encoding = enums.EncodingType.UTF16

    result = MessageToJson(client.analyze_entity_sentiment(document, encoding))

    return result


def classify_text(text):
    # [START language_classify_text]
    # print("*****************Categories Analysis*****************")
    import six
    import json
    from google.cloud import language
    from google.cloud.language import enums
    from google.cloud.language import types
    from google.protobuf.json_format import MessageToJson

    client = language.LanguageServiceClient()

    if isinstance(text, six.binary_type):
        text = text.decode('utf-8')
    document = types.Document(
        content=text.encode('utf-8'),
        type=enums.Document.Type.PLAIN_TEXT)
    try:
        categories = MessageToJson(client.classify_text(document))
    except Exception as e:
        print(e)
        return b'{}'
    return categories
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './GoogleCloudAccessKey.json'

event = {
    "Records": [
        {
            "messageId": "dc35200f-be2f-4378-93f4-4e1e606aaa70",
            "receiptHandle": "AQEBVDIux0GbLYs2+szro3bqGaHlfZVoA+AHyq281xvMYkoroEmv7fInUj5PCPZ4wgEhLgsZUdGxAJD+9QnZTGY7peHvw/yeMvWTr3KFGZGRe8mjpcZos7XJR3/jF2qYGYABHOz7wAjtjQzWq448LOKjEYNXxW5Ureg2PETziwO2K4jUajdtYavJcLumwTRaC+iGZFydnOrEDEzjsjFDwClVAjJWOdXAkM97bFvX/mqGCmoVkXoBPo9FuFxvlep4P+RqoR7Qs/v2cZiiQQQJxDNlrejHYZ8wqAT+zwyympNoW/RlH9AjzEfRNMlPRPI/sHrJas5q6LOykDYOXBgFlHQJXqqR2I1kOzqC37bK6GNCLh+iEf/toCrlWQvsLNQr8CaYavazcRlHGvZc4HVgVLZ7MJLdhWMAK0tAMSGf2EIQhd4=",
            "body": "{\"source\": \"twitter\", \"file\": \"twitter_#saudiarabia_1.json\", \"text\": null}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1552990384638",
                "SenderId": "AROAJNSSBBA3DYIAOTEII:lmb-use1-sentiment--twitter-data-ingestion",
                "ApproximateFirstReceiveTimestamp": "1552990394638"
            },
            "messageAttributes": {
                "WeeksOn": {
                    "stringValue": "6",
                    "stringListValues": [],
                    "binaryListValues": [],
                    "dataType": "Number"
                },
                "Author": {
                    "stringValue": "Dev Team",
                    "stringListValues": [],
                    "binaryListValues": [],
                    "dataType": "String"
                },
                "Title": {
                    "stringValue": "Sentiment Analysis",
                    "stringListValues": [],
                    "binaryListValues": [],
                    "dataType": "String"
                }
            },
            "md5OfMessageAttributes": "25fe00b4ee984a6fba20ee5356ce83fd",
            "md5OfBody": "ab81fd3c8eb931789a1871d2ba6b9a7d",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:069653090426:sqs-use1-sentiment-analysis",
            "awsRegion": "us-east-1"
        }
    ]
}
source, filename = (argv[1], argv[2])
lambda_handler(source, filename)
