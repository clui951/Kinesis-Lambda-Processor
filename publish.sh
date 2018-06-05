set -e

print_linebreak()
{
	printf '%*s\n' "${COLUMNS:-$(tput cols)}" '' | tr ' ' -	
}

PROD_FUNCTION="kinesis-lambda-processor-prod"
STAGING_FUNCTION="kinesis-lambda-processor-staging"
DEV_FUNCTION="kinesis-lambda-processor-dev"

ENVIRONMENT_CHOICE=""
FUNCTION_CHOICE=""

echo "Which environment would you like to publish?"
select psd in "Prod" "Staging" "Dev"; do
    case $psd in
        Prod ) ENVIRONMENT_CHOICE="Prod"; FUNCTION_CHOICE=$PROD_FUNCTION; break;;
        Staging ) ENVIRONMENT_CHOICE="Staging"; FUNCTION_CHOICE=$STAGING_FUNCTION; break;;
		Dev ) ENVIRONMENT_CHOICE="Dev"; FUNCTION_CHOICE=$DEV_FUNCTION; echo "There is currently no dev environment"; exit;;
    esac
done

echo "Are you sure you want to publish to $ENVIRONMENT_CHOICE ($FUNCTION_CHOICE)?"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) break;;
        No ) exit;;
    esac
done


print_linebreak
echo "Creating Deployment Package"

rm KinesisLambdaProcessor.zip

cd lambda
pip3 install -r requirements.txt
zip -r ../KinesisLambdaProcessor.zip *
cd ..

cd $VIRTUAL_ENV/lib/python3.6/site-packages/
zip -r $OLDPWD/KinesisLambdaProcessor.zip *
cd $OLDPWD


print_linebreak
echo "Updating lambda code and publishing new version for $FUNCTION_CHOICE"
UPDATE_FUNCTION_CODE_RESPONSE=$(aws lambda update-function-code --publish --function-name $FUNCTION_CHOICE --zip-file fileb://KinesisLambdaProcessor.zip)
echo $UPDATE_FUNCTION_CODE_RESPONSE


print_linebreak
PUBLISHED_VERSION_NUM=$(echo $UPDATE_FUNCTION_CODE_RESPONSE | grep -Eo 'Version\": \"([0-9]*)\"' | cut -d" " -f2 | tr -d '",') # grabs version number from response code; does not work with $LATEST
echo "Updating alias $FUNCTION_CHOICE:master to point to published version $PUBLISHED_VERSION_NUM"
UPDATE_ALIAS_RESPONSE=$(aws lambda update-alias --function-name $FUNCTION_CHOICE --name master --function-version $PUBLISHED_VERSION_NUM)
echo $UPDATE_ALIAS_RESPONSE
