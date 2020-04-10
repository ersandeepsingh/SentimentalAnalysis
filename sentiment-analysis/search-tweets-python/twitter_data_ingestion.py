from sys import argv
import boto3
from searchtweets import ResultStream, gen_rule_payload, load_credentials, collect_results
from datetime import datetime
from dateutil.parser import parse
import json


def data_ingestion(query):
    till_date = 201906115
    from_date = "2019-05-17"
    to_date="2019-06-15"
    results_per_call=100
    bucketname='pre-sentimental-analysis'
    premium_search_args = load_credentials("./twitter_keys.yaml",
                                           yaml_key="search_tweets_pk_api",
                                           env_overwrite=False)
    flag = True
    while flag:
        rule = gen_rule_payload(query,  from_date=from_date, to_date=to_date,
                                results_per_call=results_per_call) 
        print(rule)

        tweets = collect_results(rule, max_results=results_per_call,
                                result_stream_args=premium_search_args)

        print(len(tweets))
        all_tweets = []
        for tweet in tweets:
            date = parse(tweet['created_at'])
            # print(date)
            date_string = int("{:%Y%m%d}".format(date))
            # print(date_string)
            if date_string < till_date:
                till_date = date_string
                # print(till_date)
            all_tweets.append(json.dumps(tweet))
        to_date = str(till_date)[:4] + '-' + str(till_date)[4:6] + '-' + str(till_date)[6:]
        print(to_date)
        filename = "twitter1" + '_' + query + '_' + from_date + "_" + to_date + '.json'
        print(filename)
        with open('/tmp/' + filename, 'w') as outfile:
            outfile.write('\n'.join(all_tweets))
            upload_in_s3(filename, bucketname)
        
        if len(all_tweets)<100:
            flag = False

def upload_in_s3(filename, bucketname):
    print("inside upload_in_s3 ")
    s3 = boto3.resource('s3')
    file_loc = '/tmp/' + filename
    s3.meta.client.upload_file(file_loc, bucketname, filename)
    print("file uploaded successfully in s3.")

query = argv[1]
print(query)
data_ingestion(query)
