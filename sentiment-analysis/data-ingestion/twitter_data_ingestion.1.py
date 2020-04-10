def lambda_handler(event, context):
    from tweepy import API
    from tweepy import Cursor
    from tweepy import OAuthHandler
    # from textblob import TextBlob
    from datetime import datetime
    import config
    import boto3
    import json

    sqs = boto3.client('sqs')


    # print("event: ", event)
    # print('eventbody: ', event.get('Records')[0]['body'])
    body = json.loads(event.get('Records')[0]['body'])
    # print(body)
    hash_tags = body['topic']
    source = body['source']

    # # # # TWITTER CLIENT # # # #
    class TwitterClient():
        def __init__(self):
            # print("TwitterClient")
            self.auth = TwitterAuthenticator().authenticate_twitter_app()

        def get_tweet_based_on_hashtag(self, hashtag_list):
            # print("get_tweet_based_on_hashtag")
            tweets = []
            self.api = API(self.auth, wait_on_rate_limit=True)
            for tweet in Cursor(self.api.search, q=hashtag_list, since="2018-05-01", until="2019-04-18", tweet_mode='extended').items(10):
                tweets.append(json.dumps(tweet._json))
                # print(json.dumps(tweet._json))
            # print("tweets::", tweets)
            l = len(tweets)
            print(l)
            print(tweets[:5])
            # count = 0
            # n=0
            # while n < l:
            #   filename = source + '_' + hash_tags + '_' + str(count) + '.json'
            #   print("file_name: {}".format(filename))
            #   tweet = tweets[n:n+200]
            #   n=n+200
            #   with open('/tmp/' + filename, 'w') as outfile:
            #       outfile.write('\n'.join(tweet))
            #       upload_in_s3(filename, config.S3_BUCKET)
            #   count += 1


    # # # # TWITTER AUTHENTICATER # # # #

    class TwitterAuthenticator():

        def authenticate_twitter_app(self):
            # print("TwitterAuthenticator: authenticate_twitter_app")
            auth = OAuthHandler(config.TWITTER_CONSUMER_KEY,
                                config.TWITTER_CONSUMER_SECRET)
            auth.set_access_token(config.TWITTER_ACCESS_TOKEN,
                                  config.TWITTER_ACCESS_TOKEN_SECRET)
            return auth

    def create_sentiment():
        print("inside create")
        twitter_client = TwitterClient()
        twitter_client.get_tweet_based_on_hashtag(hash_tags)
        

    def upload_in_s3(filename, bucketname):
        s3 = boto3.resource('s3')
        file_loc = '/tmp/' + filename
        s3.meta.client.upload_file(file_loc, bucketname, filename)
        print("file uploaded successfully in s3.")
        # message_id = send_message({"source": "twitter", "file": filename, "text": None})
        # print(message_id)

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
    


    create_sentiment()

    return None


event = {
  "Records": [
    {
      "messageId": "8b67044b-8165-45ca-9938-06180c0766d8",
      "receiptHandle": "AQEBeztopVmQuoDJLtL0siMyJ+/7knrX+UxDkCI/u09p7Nonm3whvDKZYN/ZtbqJCr9XWF+/4S2ZJavqD3BKeD9zRUR8dJ8srVFpDjwawMjGVXi+pMSZ+ffLdtwNQKBYkUhhL/40bXZXXBCruidyy7W+1RwN97V1GCPtLecK4K6hWfrrj4Ob1O18ZrQX2RKvMveaHlTgFWOD2n1+jr8ff+o2p2wo1sOlua2qBAVsplaXcl6nDRLAFj7dPMx2rpnVd4016TCtUJ/2WFEevFqz7RLtPAsQ7lzV3oB8LkreItX+uelmPuvdVfS3lzNz8F6dOSqsxWBGOKJuTsveGmxvkr8h5uqwaw2Ex45zwF5fhz5oKGcD+sty9HvlBW5eP7e8jA+Ulqazn0Fsy5oS904fwuT3bRi+zVbedvA6U5kxlQ6sXek=",
      "body": "{\"source\": \"twitter\", \"topic\": \"#python\"}",
      "attributes": {
        "ApproximateReceiveCount": "2",
        "SentTimestamp": "1552984451503",
        "SenderId": "AIDAJG7U3C6DOWIJLC55Y",
        "ApproximateFirstReceiveTimestamp": "1552984461504"
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
      "md5OfBody": "8e1cd7b7014909864d28b5d2813c97d7",
      "eventSource": "aws:sqs",
      "eventSourceARN": "arn:aws:sqs:us-east-1:069653090426:sqs-use1-pre-sentiment-analysis",
      "awsRegion": "us-east-1"
    }
  ]
}
lambda_handler(event, None)
