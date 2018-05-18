import logging

from sqlalchemy import create_engine, select, Table, MetaData

# logger settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_new_engine(db_postgres_string):
    return create_engine(db_postgres_string)

# generating expected data temp table
TEMP_TABLE_BASE_NAME = 'expected_temp_table_{}'
PROCESSING_TYPE = {
    "li_code" : "m.li_code = '{}'",
    "import_id" : "im.import_record_id = '{}'"
}
EXPECTED_DATA_BASE_QUERY = (
    'CREATE TEMP TABLE {0} ON COMMIT DROP AS '
    'SELECT rd.date, substring(m.li_code, 4) as flight_id, m.creative_rtb_id::text as creative_id, SUM(rd.impressions) as impressions, SUM(rd.clicks) as clicks, MAX(ir.vendor) as provider, MAX(im.report_time_zone) as time_zone, now() as updated_at, FALSE as is_deleted '
    'FROM double_click.raw_delivery rd '
    'JOIN vendor_ids.maps m ON m.vendor_id = rd.placement_id::text AND rd.date BETWEEN m.date_start AND m.date_end '
    'JOIN double_click.import_metadata im USING (import_record_id) '
    'JOIN import.records ir ON ir.id = rd.import_record_id '
    'LEFT JOIN vendor_ids.alignment_conflicts c ON m.li_code = c.li_code AND (rd.date BETWEEN c.date_start AND c.date_end) '
    'WHERE {1} AND m.is_deleted = false AND c.li_code IS NULL '
    'group by rd.date, m.li_code, m.creative_rtb_id '
    'order by date desc;'
)

def generate_expected_data_temp_table(connection, processing_id_type, processing_id):
    temp_table_name = TEMP_TABLE_BASE_NAME.format(processing_id.replace("-","")).lower()
    where_clause_string = PROCESSING_TYPE[processing_id_type].format(processing_id)

    build_temp_table_query = EXPECTED_DATA_BASE_QUERY.format(temp_table_name, where_clause_string)

    connection.execute(build_temp_table_query)

    metadata = MetaData(connection, reflect=True)
    return Table(temp_table_name, metadata, autoload=True, autoload_with=connection)
    
# change lock timeout for current transaction
LOCK_TIMEOUT_MS = 3000
LOCK_TIMEOUT_QUERY = "SET lock_timeout = {};".format(LOCK_TIMEOUT_MS)
def set_lock_timeout_for_transaction(connection):
    connection.execute(LOCK_TIMEOUT_QUERY)

# calculate diffs against final results table
OUTPUT_SCHEMA = 'snoopy'
OUTPUT_TABLE = 'delivery_by_flight_creative_day'
def calculate_diffs_against_output_table(connection, temp_table):
    flight_ids_affected = [row[temp_table.c.flight_id] for row in select([temp_table.c.flight_id]).distinct().execute().fetchall()]
    flight_ids_affected_string = "(" + ",".join(["'" + str(id) + "'" for id in flight_ids_affected]) + ")"

    metadata = MetaData(connection, reflect=True, schema=OUTPUT_SCHEMA)
    output_table = Table(OUTPUT_TABLE, metadata, autoload=True, autoload_with=connection)

    # do deletions
    deleted_query = """
        UPDATE {0} output
        SET is_deleted = TRUE
        WHERE flight_id IN {1} AND NOT EXISTS (
            SELECT flight_id, creative_id, date FROM {2} temp
            WHERE output.flight_id = temp.flight_id AND output.creative_id = temp.creative_id AND output.date = temp.date AND output.is_deleted = FALSE
        ) RETURNING flight_id, creative_id, date;""".format(output_table.schema + "." + output_table.name, flight_ids_affected_string, temp_table.name)
    deleted = [dict(row) for row in connection.execute(deleted_query).fetchall()]

    # do update / insertion together
    delete_for_update_query = """
        DELETE
        FROM {0} output
            USING {1} temp
        WHERE output.flight_id = temp.flight_id AND output.creative_id = temp.creative_id AND output.date = temp.date;""".format(output_table.schema + "." + output_table.name, temp_table.name)
    connection.execute(delete_for_update_query)

    insert_for_update_query = """
        INSERT INTO {0} (
            SELECT *
            FROM {1} temp
        ) RETURNING *;""".format(output_table.schema + "." + output_table.name, temp_table.name)
    inserted = [dict(row) for row in connection.execute(insert_for_update_query).fetchall()]

    return (deleted, inserted)
