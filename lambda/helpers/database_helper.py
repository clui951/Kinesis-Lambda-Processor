import logging
import warnings

from sqlalchemy import exc as sa_exc
from sqlalchemy import create_engine, Table, MetaData, select, text, and_, or_, exists
from sqlalchemy.sql.functions import current_timestamp

# Logger settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_new_engine(db_postgres_string, pool_size, max_overflow):
    return create_engine(db_postgres_string, pool_size=pool_size, max_overflow=max_overflow)

# Database_helper entrypoint, called by main processor
def process_processing_id(connection, processing_id_type, processing_id):
    with connection.begin() as transaction:
        set_lock_timeout_for_transaction(connection)

        temp_table = generate_expected_data_temp_table(connection, processing_id_type, processing_id)
        s = select([temp_table.c.date, temp_table.c.flight_id, temp_table.c.creative_id, temp_table.c.impressions, temp_table.c.clicks, temp_table.c.provider, temp_table.c.time_zone, temp_table.c.is_deleted])

        flight_ids_affected = []
        if processing_id_type == LI_CODE_STRING:
            flight_ids_affected = [processing_id[3:]]
            perform_deletions = True
        else:
            flight_ids_affected = [row[temp_table.c.flight_id] for row in select([temp_table.c.flight_id]).distinct().execute().fetchall()]
            perform_deletions = False
        if not perform_deletions and not flight_ids_affected:
            # no data in the temp table
            return

        deleted, inserted = calculate_diffs_and_writes_to_output_table(connection, temp_table, flight_ids_affected, perform_deletions)


# Change lock timeout for current transaction
LOCK_TIMEOUT_MS = 3000
LOCK_TIMEOUT_QUERY = "SET lock_timeout = {};".format(LOCK_TIMEOUT_MS)
LOCK_ERROR_MESSAGE = 'lock timeout'
def set_lock_timeout_for_transaction(connection):
    connection.execute(LOCK_TIMEOUT_QUERY)


# Generating expected data temp table
TEMP_TABLE_BASE_NAME = 'expected_temp_table_{}'
LI_CODE_STRING = "li_code"
IMPORT_ID_STRING = "import_id"
CONDITION_STRING_BY_PROCESSING_ID_TYPE = {
    LI_CODE_STRING : "m.li_code = '{}'",
    IMPORT_ID_STRING : "im.import_record_id = '{}'"
}

# Temporarily hard coding DoubleClick as the provider, and not joining to import.records table
# Work is needed to fix import_record_ids such that they are consistent for double_click and import schema
BUILD_EXPECTED_DATA_TEMP_TABLE_BASE_QUERY = """
    CREATE TEMP TABLE {0} ON COMMIT DROP AS
    SELECT rd.date, substring(m.li_code, 4) as flight_id, m.creative_rtb_id::text as creative_id, SUM(rd.impressions) as impressions, SUM(rd.clicks) as clicks, 'doubleclick'::TEXT as provider,
    im.report_time_zone as time_zone, now() as updated_at, FALSE as is_deleted
    FROM double_click.raw_delivery rd
    JOIN vendor_ids.maps m ON m.vendor_id = rd.placement_id::text AND rd.date BETWEEN m.date_start AND m.date_end
    JOIN double_click.import_metadata im USING (import_record_id)
    LEFT JOIN vendor_ids.alignment_conflicts c ON m.li_code = c.li_code AND (rd.date BETWEEN c.date_start AND c.date_end)
    WHERE {1} AND m.is_deleted = false AND c.li_code IS NULL
    group by rd.date, m.li_code, m.creative_rtb_id, im.report_time_zone
    order by date desc;
"""
def generate_expected_data_temp_table(connection, processing_id_type, processing_id):
    temp_table_name = TEMP_TABLE_BASE_NAME.format(processing_id.replace("-","")).lower()
    where_clause_string = CONDITION_STRING_BY_PROCESSING_ID_TYPE[processing_id_type].format(processing_id)

    # Insert records for flights with no creative conflicts of any kind
    build_expected_data_temp_table_base_query = BUILD_EXPECTED_DATA_TEMP_TABLE_BASE_QUERY.format(temp_table_name, where_clause_string)
    connection.execute(build_expected_data_temp_table_base_query)

    # Insert records for flights with within flight creative conflict only
    insert_within_flight_creative_conflict_data_to_temp_table(connection, temp_table_name, processing_id_type, processing_id)

    metadata = MetaData(connection, reflect=True)
    return Table(temp_table_name, metadata, autoload=True, autoload_with=connection)


# This query has two conditions that are dependent upon whether or not the processing id is li_code or import_id
# See WITHIN_FLIGHT_CREATIVE_CONFLICT_QUERY_CONDITIONS_BY_PROCESSING_ID_TYPE
INSERT_WITHIN_FLIGHT_CREATIVE_CONFLICT_BASE_QUERY = """
    INSERT INTO {0} (
        SELECT rd2.date, substring(tups.li_code, 4) as flight_id, NULL as creative_id, SUM(rd2.impressions) as impressions, 
            SUM(rd2.clicks) as clicks, 'doubleclick'::TEXT as provider, im.report_time_zone as time_zone, 
            now() as updated_at, FALSE as is_deleted 
        FROM double_click.raw_delivery rd2
        JOIN (
            SELECT m.vendor_id, c.report_date AS date, m.li_code, m.vendor FROM static.calendar c 
            JOIN vendor_ids.maps m ON c.report_date BETWEEN m.date_start AND m.date_end
            JOIN {1} i on i.vendor_id =  m.vendor_id AND m.date_start <= max_date AND m.date_end >= min_date 
            JOIN vendor_ids.alignment_conflicts cf on m.li_code = cf.li_code 
                AND (c.report_date BETWEEN cf.date_start AND cf.date_end)
            WHERE m.is_deleted = FALSE AND cf.li_code = cf.li_code_2
            AND cf.li_code NOT IN (SELECT c2.li_code FROM vendor_ids.alignment_conflicts c2 WHERE c2.li_code != c2.li_code_2)
            GROUP BY 1, 2, 3, 4
            ) tups ON rd2.placement_id::text = tups.vendor_id AND rd2.date = tups.date
        JOIN double_click.import_metadata im ON rd2.import_record_id = im.import_record_id
        WHERE {2}
        GROUP BY rd2.date, tups.li_code, im.report_time_zone
    );
"""
RELEVANT_ID_MAPS_FOR_LI_CODE =   """(
                            SELECT vendor_id, MIN(date_start) as min_date, MAX(date_end) as max_date FROM vendor_ids.maps
                            WHERE li_code = '{0}'
                            GROUP BY 1 )
                        """
LI_CODE_CONDITION =   """ 
                            tups.li_code = '{0}' 
                        """
RELEVANT_ID_MAPS_FOR_IMPORT_ID = """ (
                            SELECT placement_id::text as vendor_id, MIN(date) as min_date , MAX(date) as max_date 
                                FROM double_click.raw_delivery
                            WHERE import_record_id = {0}
                            GROUP BY 1 )
                        """
IMPORT_ID_CONDITION = """ 
                            rd2.import_record_id = {0} 
                        """
WITHIN_FLIGHT_CREATIVE_CONFLICT_QUERY_CONDITIONS_BY_PROCESSING_ID_TYPE = {
    LI_CODE_STRING : (RELEVANT_ID_MAPS_FOR_LI_CODE, LI_CODE_CONDITION),
    IMPORT_ID_STRING : (RELEVANT_ID_MAPS_FOR_IMPORT_ID, IMPORT_ID_CONDITION)
}
# Inserts records for flights with within flight creative conflicts only
def insert_within_flight_creative_conflict_data_to_temp_table(connection, temp_table_name, processing_id_type, processing_id):
    conditional_query_tuple = WITHIN_FLIGHT_CREATIVE_CONFLICT_QUERY_CONDITIONS_BY_PROCESSING_ID_TYPE[processing_id_type]
    insert_within_flight_creative_conflict_query = INSERT_WITHIN_FLIGHT_CREATIVE_CONFLICT_BASE_QUERY.format(
        temp_table_name,
        conditional_query_tuple[0].format(processing_id),
        conditional_query_tuple[1].format(processing_id)
    )
    connection.execute(insert_within_flight_creative_conflict_query)


# Calculate diffs against final results table
OUTPUT_SCHEMA = 'snoopy'
OUTPUT_TABLE = 'delivery_by_flight_creative_day'
def calculate_diffs_and_writes_to_output_table(connection, temp_table, flight_ids_affected, perform_deletions):
    flight_ids_affected_string = "(" + ",".join(["'" + str(id) + "'" for id in flight_ids_affected]) + ")"

    # Filter warnings due to partial index reflection in SqlAlchemy
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)

        metadata = MetaData(connection, reflect=True, schema=OUTPUT_SCHEMA)
        output_table = Table(OUTPUT_TABLE, metadata, autoload=True, autoload_with=connection)

    # Lock rows; lock timeout should be caught, and force a retry
    output_table.select().where(output_table.c.flight_id.in_([str(id) for id in flight_ids_affected])).with_for_update().execute()

    deleted = []
    if perform_deletions:
        # Do deletions
        deleted_query = output_table.update().returning(output_table.c.flight_id, output_table.c.creative_id, output_table.c.date).where(
            and_(
                output_table.c.flight_id.in_([str(id) for id in flight_ids_affected]),
                ~exists().where(
                    and_(
                        output_table.c.flight_id == temp_table.c.flight_id,
                        or_(
                            output_table.c.creative_id == temp_table.c.creative_id,
                            output_table.c.creative_id.isnot_distinct_from(temp_table.c.creative_id)
                        ),
                        output_table.c.date == temp_table.c.date
                    )
                )
            )
        ).values(is_deleted = True, updated_at = current_timestamp())
        deleted = [dict(row) for row in deleted_query.execute().fetchall()]

    # Do updates / insertions together
    delete_for_update_query = output_table.delete().where(
        and_(
            output_table.c.flight_id == temp_table.c.flight_id,
            or_(
                output_table.c.creative_id == temp_table.c.creative_id,
                output_table.c.creative_id.isnot_distinct_from(temp_table.c.creative_id)
            ),
            output_table.c.date == temp_table.c.date
        )
    )
    delete_for_update_query.execute()

    insert_for_update_query = output_table.insert().returning(text('*')).from_select(temp_table.c, temp_table.select())
    inserted = [dict(row) for row in insert_for_update_query.execute().fetchall()]

    return (deleted, inserted)