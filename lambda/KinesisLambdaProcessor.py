import base64
import json
import logging
import sys
import traceback

sys.path.insert(0, "thirdpartylib/")
import psycopg2
from sqlalchemy import create_engine, Table, MetaData

import rds_config

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

# Expected messages handling
PROCESSING_ID_TYPE_JSON_HEADER = 'processing_id_type'
PROCESSING_ID_JSON_HEADER = 'processing_id'

# db temp table 
EXPECTED_DATA_BASE_QUERY = (
    'CREATE TEMP TABLE {0} ON COMMIT DROP AS '
    'SELECT rd.date, substring(m.li_code, 4) as flight_id, m.creative_rtb_id as creative_id, SUM(rd.impressions) as impressions, SUM(rd.clicks) as clicks, MAX(ir.vendor) as provider, MAX(im.report_time_zone) as time_zone, now() as updated_at, FALSE as is_deleted '
    'FROM double_click.raw_delivery rd '
    'JOIN vendor_ids.maps m ON m.vendor_id = rd.placement_id::text AND rd.date BETWEEN m.date_start AND m.date_end '
    'JOIN double_click.import_metadata im USING (import_record_id) '
    'JOIN import.records ir ON ir.id = rd.import_record_id '
    'LEFT JOIN vendor_ids.alignment_conflicts c ON m.li_code = c.li_code AND (rd.date BETWEEN c.date_start AND c.date_end) '
    'WHERE {1} AND m.is_deleted = false AND c.li_code IS NULL '
    'group by rd.date, m.li_code, m.creative_rtb_id '
    'order by date desc;'
)
TEMP_TABLE_BASE_NAME = 'expected_temp_table_{}'

PROCESSING_TYPE = {
    "li_code" : "m.li_code = '{}'",
    "import_id" : "im.import_record_id = '{}'"
}

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

            generate_expected_state(processing_id_type, processing_id)
    
    except Exception as e:
        logger.error(traceback.format_exc())
        return 'Failed to process {} records'.format(len(event['Records']))

    return 'Successfully processed {} records.'.format(len(event['Records']))

def get_connection():
	return engine.connect()

def generate_expected_state(processing_id_type, processing_id):
    connection = get_connection()
    with connection.begin() as transaction:
        temp_table_name = TEMP_TABLE_BASE_NAME.format(processing_id.replace("-","")).lower()

        # build temp table with expected data
        where_clause_string = PROCESSING_TYPE[processing_id_type].format(processing_id)
        build_temp_table_query = EXPECTED_DATA_BASE_QUERY.format(temp_table_name, where_clause_string)
        print(build_temp_table_query)
        connection.execute(build_temp_table_query)

        metadata = MetaData(engine, reflect=True)
        temp_table = Table(temp_table_name, metadata, autoload=True, autoload_with=connection)

        # select from table
        result = connection.execute(temp_table.select())
        print(result.fetchall()[0])
