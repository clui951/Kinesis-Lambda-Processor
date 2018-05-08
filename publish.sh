rm KinesisLambdaProcessor.zip

cd lambda
pip3 install -r requirements.txt
zip -r ../KinesisLambdaProcessor.zip *
cd ..

cd $VIRTUAL_ENV/lib/python3.6/site-packages/
zip -r $OLDPWD/KinesisLambdaProcessor.zip *
cd $OLDPWD

aws lambda update-function-code --function-name kinesis-lambda-processor --zip-file fileb://KinesisLambdaProcessor.zip