import logging

from sqlalchemy import Table, MetaData

# logger settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# temp table for processing
TEMP_TABLE_BASE_NAME = 'expected_temp_table_{}'
PROCESSING_TYPE = {
    "li_code" : "m.li_code = '{}'",
    "import_id" : "im.import_record_id = '{}'"
}

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

LOCK_TIMEOUT_MS = 2000
LOCK_TIMEOUT_QUERY = "SET lock_timeout = {};".format(LOCK_TIMEOUT_MS)

def generate_expected_data_temp_table(processing_id_type, processing_id):
    temp_table_name = TEMP_TABLE_BASE_NAME.format(processing_id.replace("-","")).lower()
    # build temp table with expected data
    where_clause_string = PROCESSING_TYPE[processing_id_type].format(processing_id)
    build_temp_table_query = EXPECTED_DATA_BASE_QUERY.format(temp_table_name, where_clause_string)
    print(build_temp_table_query)
    connection.execute(build_temp_table_query)

    metadata = MetaData(engine, reflect=True)
    temp_table = Table(temp_table_name, metadata, autoload=True, autoload_with=connection)
    