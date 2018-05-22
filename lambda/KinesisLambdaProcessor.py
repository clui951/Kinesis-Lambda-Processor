import base64
import json
import logging
import os
import sys
import traceback

if os.getenv('env') in ['production', 'staging']:
    sys.path.insert(0, "thirdpartylib/")
import psycopg2
from sqlalchemy.exc import OperationalError

from config import db_config 
from helper.database_helper import (create_new_engine, process_processing_id)

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

LOCK_ERROR_MESSAGE = 'lock timeout'
MAXIMUM_RETRY_ON_DEADLOCK = 3

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

            # attempt to process, retrying up to MAXIMUM_RETRY_ON_DEADLOCK times
            retries_left = MAXIMUM_RETRY_ON_DEADLOCK
            while retries_left >= 0:
                try:
                    process_processing_id(get_connection(), processing_id_type, processing_id)
                    break;
                except OperationalError as e:
                    if LOCK_ERROR_MESSAGE in traceback.format_exc() and retries_left > 0:
                        logger.warn('Lock timeout trying to process {0}. Number of attempts left: {1}'.format(decoded_payload, retries_left))
                    else:
                        raise
                retries_left -= 1
    
    except Exception as e:
        logger.error(traceback.format_exc())
        return 'Failed to process {} records'.format(len(event['Records']))

    return 'Successfully processed {} records.'.format(len(event['Records']))

def get_connection():
    return engine.connect()
