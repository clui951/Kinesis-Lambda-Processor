import base64
import json
import logging
import sys
import traceback

sys.path.insert(0, "thirdpartylib/")
import psycopg2
from sqlalchemy import create_engine, Table, MetaData

from config import db_config 
from helper.database_helper import create_new_engine, generate_expected_data_temp_table

##### below setup will load on every new Execution Context container #####
##### 'cold' functions will setup a new container
##### 'warm' functions will reuse the previously setup container

# logger settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Loading new Execution Context')

# db settings
db_endpoint  = db_config.db_endpoint
db_username = db_config.db_username
db_password = db_config.db_password
db_name = db_config.db_name
db_postgres_string = "postgres://" + db_username + ":" + db_password + "@" + db_endpoint + "/" + db_name

logger.info("Creating new database engine: " + db_postgres_string)
engine = create_new_engine(db_postgres_string)

# Expected messages handling
PROCESSING_ID_TYPE_JSON_HEADER = 'processing_id_type'
PROCESSING_ID_JSON_HEADER = 'processing_id'

def lambda_handler(event, context):    
    # wrap all processing within try/except because we don't want failures to halt further processing
    try:
        # we only expect one record to arrive at a time, but leaving this loop for best practice
        for record in event['Records']:
            # Kinesis data is base64 encoded so decode here
            payload = base64.b64decode(record['kinesis']['data'])
            decoded_payload = payload.decode("utf-8")
            logger.info("Processing payload: " + decoded_payload)

            json_payload = json.loads(decoded_payload)
            processing_id_type = json_payload[PROCESSING_ID_TYPE_JSON_HEADER]
            processing_id = json_payload[PROCESSING_ID_JSON_HEADER]

            process_processing_id(processing_id_type, processing_id)
    
    except Exception as e:
        logger.error(traceback.format_exc())
        return 'Failed to process {} records'.format(len(event['Records']))

    return 'Successfully processed {} records.'.format(len(event['Records']))

def process_processing_id(processing_id_type, processing_id):
    connection = get_connection()
    with connection.begin() as transaction:
        temp_table = generate_expected_data_temp_table(processing_id_type, processing_id, connection)
    
        # select from table
        result = connection.execute(temp_table.select())
        print(result.fetchall()[0])

def get_connection():
    return engine.connect()


