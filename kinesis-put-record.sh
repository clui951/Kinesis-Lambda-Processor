set -e

print_linebreak()
{
	printf '%*s\n' "${COLUMNS:-$(tput cols)}" '' | tr ' ' -	
}

echo "Which kinesis stream would you like to put-record?"
select psd in "kinesis-lambda-stream"; do
    case $psd in
        kinesis-lambda-stream ) KINESIS_STREAM="kinesis-lambda-stream"; break;;
    esac
done

echo -e "\nWhich processing_id_type would you like to put to $KINESIS_STREAM?"
select psd in "li_code" "import_id"; do
    case $psd in
        li_code ) PROCESSING_ID_TYPE="li_code"; break;;
		import_id ) PROCESSING_ID_TYPE="import_id"; break;;
    esac
done

echo -e "\nWhat $PROCESSING_ID_TYPE would you like to put to $KINESIS_STREAM?"
read PROCESSING_ID


print_linebreak
RECORD="'{\"processing_id_type\":\"$PROCESSING_ID_TYPE\",\"processing_id\":\"$PROCESSING_ID\"}'"
echo "Are you sure you want to put $RECORD to $KINESIS_STREAM?"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) break;;
        No ) exit;;
    esac
done


print_linebreak
echo -e "Putting $RECORD to $KINESIS_STREAM\n"

PARTITION_KEY="'$PROCESSING_ID'"

COMMAND="aws kinesis put-record --profile default --stream-name DS-snoopy-kinesis-stream --partition-key $PARTITION_KEY --data $RECORD"
echo "\$ $COMMAND"
PUT_RECORD_RESPONSE=$($COMMAND)
echo $PUT_RECORD_RESPONSE
echo