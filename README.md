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

### How to Run Tests
__Note:__ First makesure you have sourced your virtual environment (see above)
```
$ cd lambda/
$ python -m pytest tests/
$ python -m pytest tests/ -s  # To see stdout
```

#### Why is Psycopg2 Dependency Already Included
Psycopg2 is a compiled module, but AWS Lambda does not have the required PostgreSQL libraries in the AMI image to do so. We need to include a version that has been statically pre-compiled on an Amazon Linux machine.
https://github.com/jkehler/awslambda-psycopg2 (use psycopg2-3.6 for python3.6)
