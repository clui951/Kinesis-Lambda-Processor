# kinesis-lambda-processor
Repo for a Lambda Processor that consumes a Kinesis stream and processes on a RDS database.


### To Setup Virtual Environment
Create a virtual environment and source it  
```
$ virtualenv -p python3 virtualenv
$ source virtualenv/bin/activate
```
Install requirements
```
$ pip3 install -r lambda/requirements.txt
```

### To Create & Publish Deployment Package
__Note:__ First make sure you have sourced your virtual environment (see above)
```
$ ./publish.sh
```

### Production/Staging Lambda Environment
2 separate lambda functions set up:  
[kinesis-lambda-processor-staging]()  
[kinesis-lambda-processor-prod]()  
When the publish script is run, a new deployment package is shipped to the corresponding lambda function, a new version is published, and the function alias `master` points to the new published version.  

Logs can be found in CloudWatch > Log Groups > /aws/lambda/kinesis-lambda-processor-\<env\>

### Consuming from Kinesis Queue
The Kinesis stream that the processor consumes from is: Kinesis-Lambda-Event-Stream  
Example write to queue using aws cli:  
```
$ aws kinesis put-record --profile default --stream-name Kinesis-Lambda-Event-Stream --partition-key 1468224 --data '{"processing_id_type":"import_id","processing_id":"1468224"}'
$ aws kinesis put-record --profile default --stream-name Kinesis-Lambda-Event-Stream --partition-key 'LI-568467' --data '{"processing_id_type":"li_code","processing_id":"LI-568467"}'
```

### How to Run Tests Locally
#### Through Docker
```
$ make clean
$ make build
$ make test
```
#### Through local environment / command line
__Note:__ First makesure you have sourced your virtual environment (see above)
```
$ cd lambda/
$ python -m pytest tests/
$ python -m pytest tests/ -s -v  # To see stdout and details
$ python -m pytest tests/path/to/test.py::test_name -s -v # To run specific test in file
```

### Why is Psycopg2 Dependency Already Included
Psycopg2 is a compiled module, but AWS Lambda does not have the required PostgreSQL libraries in the AMI image to do so. We need to include a version that has been statically pre-compiled on an Amazon Linux machine.
https://github.com/jkehler/awslambda-psycopg2 (use psycopg2-3.6 for python3.6)
