from __future__ import print_function
from sqlalchemy import create_engine

import base64
import json
import logging
import psycopg2
import rds_config
import sys

##### below setup will load on every new Execution Context container #####
##### 'cold' functions will setup a new container
##### 'warm' functions will reuse the previously setup container 

# logger settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Loading new Execution Context')

# db settings
db_endpoint  = rds_config.db_endpoint
db_username = rds_config.db_username
db_password = rds_config.db_password
db_name = rds_config.db_name

# db connection engine
db_postgres_string = "postgres://" + db_username + ":" + db_password + "@" + db_endpoint + "/" + db_name
logger.info("Creating new database engine: " + db_postgres_string)
engine = create_engine(db_postgres_string)
logger.info("Database engine created")

def lambda_handler(event, context):
    # logger.debug("Received event: " + json.dumps(event, indent=2))
    
    # we only expect one record to arrive at a time, but leaving this loop for best practice
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data'])
        decoded_payload = payload.decode("utf-8")
        logger.info("Decoded payload: " + decoded_payload)

        connection = get_connection()
        logger.info("Connection acquired")
        result = connection.execute("select * from delta.stats_by_flight where li_code = '{}' order by date desc;".format(decoded_payload))
        logger.info("Query executed. Results: ")
        print(result.fetchall())

    return 'Successfully processed {} records.'.format(len(event['Records']))



def get_connection():
	return engine.connect()