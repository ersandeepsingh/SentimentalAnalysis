# sentiment analysis API 

## Install guide

##### Clone the repo

```bash
$ git clone git@github.com:triconinfotech/sentiment-analysis.git
$ cd sentiment-analysis
```
```
create a virtual env
$ virtualenv sentiment-analysis-venv
$ source sentiment-analysis-venv/bin/activate
```


##### Install dependencies
```bash
$ pip install -r requirements.txt
```

##### Run the app
```bash
$ python run_app.py
```

## Running the app

```bash
$ python run_app.py
```


## Use postman to make a request
```
http://localhost:8080/sentiment/create
{
    "source": "twitter",
    "topic":"#Saudivision2030"
}
```



## Post Steps

```
data-ingestion ----> analysis ----> report
```

## Please find dummy tweet json file 
```
tmp1/README.md
```
