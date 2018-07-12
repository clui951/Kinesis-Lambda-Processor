import pytest
import traceback
import datetime
from operator import itemgetter

from sqlalchemy.exc import OperationalError
from sqlalchemy import select, MetaData, Table

from config import db_config
from helpers import database_helper as h

OUTPUT_TABLE_FULL_NAME = h.OUTPUT_SCHEMA + "." + h.OUTPUT_TABLE

###########################
##### Pytest Fixtures #####
###########################


@pytest.fixture(scope="module")
def engine():
    db_endpoint = db_config.db_test_endpoint
    db_username = db_config.db_username
    db_password = db_config.db_password
    db_name = db_config.db_name
    db_postgres_string = "postgres://" + db_username + ":" + db_password + "@" + db_endpoint + "/" + db_name
    return h.create_new_engine(db_postgres_string, 5, 10)


@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown_for_entire_test_suite(engine):
    # Setup before starting entire test suite
    reset_upstream_tables(engine.connect())
    truncate_output_table(engine.connect())
    yield
    # Teardown after ending entire test suite
    truncate_all_tables(engine.connect())


@pytest.fixture(scope="function")
def connection(engine):
    return engine.connect()


@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown_for_each_test(connection):
    # Setup before each test
    reset_upstream_tables(connection)
    insert_standard_output_data(connection)
    yield
    # Teardown after each test
    truncate_output_table(connection)


#################
##### Tests #####
#################

def test_process_flight_id_with_empty_table_populate_expected_output(connection):
    truncate_output_table(connection)
    h.process_processing_id(connection, 'li_code', 'LI-123456')
    h.process_processing_id(connection, 'li_code', 'LI-7891011')

    results = select_all_from_output_table(connection)
    assert get_standard_output_data() == results


def test_process_import_id_with_empty_table_populate_expected_output(connection):
    truncate_output_table(connection)
    h.process_processing_id(connection, 'import_id', '1')

    results = select_all_from_output_table(connection)
    assert get_standard_output_data() == results


def test_process_flight_id_with_deleted_rows_populate_marks_as_deleted(connection):
    connection.execute("""
            INSERT INTO {} (date, flight_id, creative_id, impressions, clicks, provider, time_zone, is_deleted) 
            VALUES ('2018-05-05', '123456', '1111111', 999, 999, 'doubleclick', 'America/New_York', 'f');
        """.format(OUTPUT_TABLE_FULL_NAME))

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    expected = get_standard_output_data()
    expected.add((datetime.date(2018, 5, 5), '123456', '1111111', 999, 999, 'doubleclick', 'America/New_York', True))
    assert expected == results


def test_process_flight_id_with_undeleted_rows_populate_marks_as_not_deleted(connection):
    connection.execute("UPDATE {} SET is_deleted = TRUE WHERE flight_id = '123456';".format(OUTPUT_TABLE_FULL_NAME))

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    assert get_standard_output_data() == results


def test_process_flight_id_with_updated_data_populate_updates_data(connection):
    connection.execute("UPDATE {} SET clicks = 0 WHERE flight_id = '123456';".format(OUTPUT_TABLE_FULL_NAME))

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    assert get_standard_output_data() == results


def test_process_flight_id_with_new_data_populate_new_data(connection):
    connection.execute("DELETE FROM {} WHERE flight_id = '123456' AND date = '2018-05-01';".format(OUTPUT_TABLE_FULL_NAME))

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    assert get_standard_output_data() == results


def test_process_flight_ids_with_deleted_vendor_ids_maps_populate_marks_as_deleted(connection):
    truncate_output_table(connection)
    connection.execute("UPDATE vendor_ids.maps SET is_deleted = TRUE WHERE li_code = 'LI-123456';")

    h.process_processing_id(connection, 'li_code', 'LI-123456')
    h.process_processing_id(connection, 'li_code', 'LI-7891011')

    results = select_all_from_output_table(connection)
    standard_results = get_standard_output_data()
    expected_results = set()
    for tup in standard_results:
        if tup[1] == '123456':
            continue
        expected_results.add(tup)
    assert  expected_results == results


def test_process_import_id_with_no_resulting_data_populate_nothing(connection):
    truncate_output_table(connection)
    connection.execute("UPDATE vendor_ids.maps SET is_deleted = TRUE;")

    h.process_processing_id(connection, 'import_id', '1')

    results = select_all_from_output_table(connection)
    assert  set() == results


def test_process_flight_id_with_no_resulting_data_populate_deletes_all_corresponding(connection):
    # if no resulting data generated from upstream, previously corresponding data must be deleted
    connection.execute("UPDATE vendor_ids.maps SET is_deleted = TRUE WHERE li_code = 'LI-123456';")

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    standard_results = get_standard_output_data()
    expected_results = set()
    for tup in standard_results:
        if tup[1] == '123456':
            tuplst = list(tup)
            tuplst[-1] = True
            tup = tuple(tuplst)
        expected_results.add(tup)
    assert  expected_results == results


def test_process_flight_id_with_alignment_conflict_populate_deletes_corresponding(connection):
    connection.execute("""INSERT INTO vendor_ids.alignment_conflicts (li_code, date_start, date_end)
                            VALUES ('LI-123456', '2018-04-30', '2018-05-03');""")

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    standard_results = get_standard_output_data()
    expected_results = set()
    for tup in standard_results:
        if tup[1] == '123456':
            tuplst = list(tup)
            tuplst[-1] = True
            tup = tuple(tuplst)
        expected_results.add(tup)
    assert  expected_results == results


def test_process_flight_id_with_within_flight_creative_conflict_populates_expected(connection):
    truncate_output_table(connection)
    connection.execute("TRUNCATE vendor_ids.maps;")
    connection.execute("TRUNCATE vendor_ids.alignment_conflicts;")
    vendor_ids_maps_insert_within_flight_conflict_query = """
                INSERT INTO vendor_ids.maps (li_code, creative_rtb_id, date_start, date_end, vendor, vendor_id, is_deleted)
                VALUES
                    ('LI-123456', 1111111, '2018-04-30', '2018-05-03', 'doubleclick', '12121212', 'f'),
                    ('LI-123456', 2222222, '2018-04-30', '2018-05-03', 'doubleclick', '12121212', 'f');
            """
    connection.execute(vendor_ids_maps_insert_within_flight_conflict_query)
    vendor_ids_alignment_conflict_within_flight_conflict_query = """
                INSERT INTO vendor_ids.alignment_conflicts (li_code, li_code_2, date_start, date_end, creative_ids)
                VALUES ('LI-123456', 'LI-123456', '2018-04-30', '2018-05-03', ARRAY[1111111, 2222222]);
            """
    connection.execute(vendor_ids_alignment_conflict_within_flight_conflict_query)

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    result_dates = set()

    for result in results:
        assert result[1] == '123456'
        assert result[2] == None
        result_dates.add(result[0])
    assert result_dates == set([datetime.date(2018, 4 , 30), datetime.date(2018, 5 , 1), datetime.date(2018, 5 , 2), datetime.date(2018, 5 , 3)])


def test_process_flight_id_with_within_flight_creative_conflict_twice_no_error(connection):
    truncate_output_table(connection)
    connection.execute("TRUNCATE vendor_ids.maps;")
    connection.execute("TRUNCATE vendor_ids.alignment_conflicts;")
    vendor_ids_maps_insert_within_flight_conflict_query = """
                INSERT INTO vendor_ids.maps (li_code, creative_rtb_id, date_start, date_end, vendor, vendor_id, is_deleted)
                VALUES
                    ('LI-123456', 1111111, '2018-04-30', '2018-05-03', 'doubleclick', '12121212', 'f'),
                    ('LI-123456', 2222222, '2018-04-30', '2018-05-03', 'doubleclick', '12121212', 'f');
            """
    connection.execute(vendor_ids_maps_insert_within_flight_conflict_query)
    vendor_ids_alignment_conflict_within_flight_conflict_query = """
                INSERT INTO vendor_ids.alignment_conflicts (li_code, li_code_2, date_start, date_end, creative_ids)
                VALUES ('LI-123456', 'LI-123456', '2018-04-30', '2018-05-03', ARRAY[1111111, 2222222]);
            """
    connection.execute(vendor_ids_alignment_conflict_within_flight_conflict_query)

    h.process_processing_id(connection, 'li_code', 'LI-123456')
    h.process_processing_id(connection, 'li_code', 'LI-123456')


def test_process_import_id_within_flight_creative_conflict_populates_expected(connection):
    truncate_output_table(connection)
    vendor_ids_maps_set_within_flight_conflict_query = """
                    UPDATE vendor_ids.maps SET vendor_id = '12121212' WHERE li_code = 'LI-123456';
                """
    connection.execute(vendor_ids_maps_set_within_flight_conflict_query)
    vendor_ids_alignment_conflict_within_flight_conflict_query = """
                    INSERT INTO vendor_ids.alignment_conflicts (li_code, li_code_2, date_start, date_end, creative_ids)
                    VALUES ('LI-123456', 'LI-123456', '2018-04-30', '2018-05-03', ARRAY[1111111, 2222222]);
                """
    connection.execute(vendor_ids_alignment_conflict_within_flight_conflict_query)

    h.process_processing_id(connection, 'import_id', '1')

    standard_output_data_flight_7891011 = get_standard_output_data_flight7891011()
    results = select_all_from_output_table(connection)
    for result in results:
        if result[1] == '123456':
            assert result[2] == None
        if result[2] == '7891011':
            assert result in standard_output_data_flight_7891011


def test_process_flight_id_both_within_and_not_within_flight_creative_conflict_populates_none(connection):
    truncate_output_table(connection)
    connection.execute("TRUNCATE vendor_ids.maps;")
    connection.execute("TRUNCATE vendor_ids.alignment_conflicts;")
    vendor_ids_maps_insert_within_flight_conflict_query = """
                INSERT INTO vendor_ids.maps (li_code, creative_rtb_id, date_start, date_end, vendor, vendor_id, is_deleted)
                VALUES
                    ('LI-123456', 1111111, '2018-04-30', '2018-05-03', 'doubleclick', '12121212', 'f'),
                    ('LI-123456', 2222222, '2018-04-30', '2018-05-03', 'doubleclick', '12121212', 'f');
            """
    connection.execute(vendor_ids_maps_insert_within_flight_conflict_query)
    vendor_ids_alignment_conflict_within_flight_conflict_query = """
                INSERT INTO vendor_ids.alignment_conflicts (li_code, li_code_2, date_start, date_end, creative_ids)
                VALUES ('LI-123456', 'LI-123456', '2018-04-30', '2018-05-03', ARRAY[1111111, 2222222]),
                        ('LI-123456', 'LI-999999', '2018-04-30', '2018-05-03', ARRAY[1111111, 9999999]);
            """
    connection.execute(vendor_ids_alignment_conflict_within_flight_conflict_query)

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    assert results == set()


def test_process_flight_id_with_previous_within_flight_creative_conflict_populates_expected(connection):
    truncate_output_table(connection)
    # add previous within flight creative conflict record to output table
    connection.execute("""
                    INSERT INTO {} (date, flight_id, creative_id, impressions, clicks, provider, time_zone, is_deleted)
                    VALUES ('2018-05-01', '123456', NULL, 999, 999, 'doubleclick', 'America/New_York', 'f');
                """.format(OUTPUT_TABLE_FULL_NAME))

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    expected = get_standard_output_data_flight123456()
    expected.add((datetime.date(2018, 5, 1), '123456', None, 999, 999, 'doubleclick', 'America/New_York', True))
    assert expected == results


def test_generate_expected_data_temp_table_processing_id_creates_correct_temp_table(connection):
    with connection.begin() as transaction:
        temp_table = h.generate_expected_data_temp_table(connection, 'import_id', '1')
        s = select([temp_table.c.date, temp_table.c.flight_id, temp_table.c.creative_id, temp_table.c.impressions, temp_table.c.clicks, temp_table.c.provider, temp_table.c.time_zone, temp_table.c.is_deleted])
        assert get_standard_output_data() == {tuple(rowproxy.values()) for rowproxy in connection.execute(s).fetchall()}


def test_calculate_diffs_and_writes_to_output_table_temp_table_and_do_perform_deletions_returns_correct_diffs(connection):
    with connection.begin() as transaction:
        # build temp table
        connection.execute(""" 
                SELECT * INTO TEMPORARY TABLE temp_table
                FROM {}
                WHERE flight_id = '7891011';
            """.format(OUTPUT_TABLE_FULL_NAME))

        # change output table
        connection.execute("""
                INSERT INTO {} (date, flight_id, creative_id, impressions, clicks, provider, time_zone, is_deleted) 
                VALUES ('2018-05-05', '7891011', '1111111', 999, 999, 'doubleclick', 'America/New_York', 'f');
            """.format(OUTPUT_TABLE_FULL_NAME))

        # load temp table and calculate diffs
        metadata = MetaData(connection, reflect=True)
        temp_table = Table("temp_table", metadata, autoload=True, autoload_with=connection)
        deleted, inserted = h.calculate_diffs_and_writes_to_output_table(connection, temp_table, [7891011], True)

        assert deleted == [{'flight_id' : '7891011' , 'creative_id' : '1111111', 'date' : datetime.date(2018, 5, 5)}]

        expected_inserted = [
                        {'date': datetime.date(2018, 4, 30), 'flight_id':'7891011', 'creative_id':'3333333', 'impressions':1809, 'clicks':2, 'provider':'doubleclick', 'time_zone':'America/New_York', 'updated_at':None, 'is_deleted': False},
                        {'date': datetime.date(2018, 4, 30), 'flight_id':'7891011', 'creative_id':'4444444', 'impressions':19032, 'clicks':4, 'provider':'doubleclick','time_zone':'America/New_York', 'updated_at':None, 'is_deleted': False},
                        {'date': datetime.date(2018, 4, 30), 'flight_id':'7891011', 'creative_id':'5555555', 'impressions':5588, 'clicks':1, 'provider':'doubleclick', 'time_zone':'America/New_York', 'updated_at':None, 'is_deleted': False}
                    ]

        # sort both lists for comparison; using impressions, but any unique value will be okay
        inserted, expected_inserted = [sorted(l, key=itemgetter('impressions'))
                      for l in (inserted, expected_inserted)]

        assert inserted == expected_inserted


def test_set_lock_timeout_for_transaction_timeout_with_expected_error(engine):
    locking_connection = engine.connect()
    blocked_connection = engine.connect()

    with locking_connection.begin() as transaction:
        locking_connection.execute("SELECT * FROM {} FOR UPDATE".format(OUTPUT_TABLE_FULL_NAME))
        h.set_lock_timeout_for_transaction(blocked_connection)
        with pytest.raises(OperationalError):
            try:
                blocked_connection.execute("SELECT * FROM {} FOR UPDATE".format(OUTPUT_TABLE_FULL_NAME))
            except OperationalError as e:
                if h.LOCK_ERROR_MESSAGE in traceback.format_exc():
                    raise


def test_upsert_with_unique_key(connection):
    connection.execute("""
            INSERT INTO {} (date, flight_id, creative_id, impressions, clicks, provider, time_zone, is_deleted) 
            VALUES 
                ('2018-05-05', '123456', '1111111', 999, 999, 'doubleclick', 'America/New_York', 'f'),
                ('2018-05-05', '123456', '1111111', 222, 222, 'doubleclick', 'Europe/London', 'f');
        """.format(OUTPUT_TABLE_FULL_NAME))

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    expected = get_standard_output_data()
    expected.add((datetime.date(2018, 5, 5), '123456', '1111111', 999, 999, 'doubleclick', 'America/New_York', True))
    expected.add((datetime.date(2018, 5, 5), '123456', '1111111', 222, 222, 'doubleclick', 'Europe/London', True))
    assert expected == results


def test_upsert_with_null_creative_id(connection):
    connection.execute("""
            INSERT INTO {} (date, flight_id, creative_id, impressions, clicks, provider, time_zone, is_deleted) 
            VALUES 
                ('2018-05-05', '123456', null, 999, 999, 'doubleclick', 'America/New_York', 'f'),
                ('2018-05-05', '123456', null, 222, 222, 'doubleclick', 'Europe/London', 'f');
        """.format(OUTPUT_TABLE_FULL_NAME))

    h.process_processing_id(connection, 'li_code', 'LI-123456')

    results = select_all_from_output_table(connection)
    expected = get_standard_output_data()
    expected.add((datetime.date(2018, 5, 5), '123456', None, 999, 999, 'doubleclick', 'America/New_York', True))
    expected.add((datetime.date(2018, 5, 5), '123456', None, 222, 222, 'doubleclick', 'Europe/London', True))
    assert expected == results


##########################
##### Helper Methods #####
##########################
def reset_upstream_tables(connection):
    truncate_all_tables(connection)
    load_upstream_table_data(connection)


def load_upstream_table_data(connection):
    doubleclick_raw_delivery_insert_query = """
            INSERT INTO double_click.raw_delivery (import_record_id, placement_id, "date", impressions, clicks, campaign_id, ad_id, advertiser, advertiser_id, campaign, placement_rate, site_keyname)
            VALUES
                (1, 12121212, '2018-05-03', 50714, 7, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 23232323, '2018-05-03', 1044, 0, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 12121212, '2018-05-02', 58977, 3, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 23232323, '2018-05-02', 905, 2, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 12121212, '2018-05-01', 42303, 3, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 23232323, '2018-05-01', 2736, 2, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 12121212, '2018-04-30', 43841, 4, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 23232323, '2018-04-30', 2941, 2, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 34343434, '2018-04-30', 1809, 2, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 45454545, '2018-04-30', 19032, 4, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
                (1, 56565656, '2018-04-30', 5588, 1, 000, 000, 'junk', 000, 'junk', 000, 'junk');
        """
    connection.execute(doubleclick_raw_delivery_insert_query)

    doubleclick_import_metadata_insert_query = """
            INSERT INTO double_click.import_metadata (import_record_id, report_time_zone, s3_path, credential, profile_id)
            VALUES
                (1, 'America/New_York', 'junk', 'junk', 0);
        """
    connection.execute(doubleclick_import_metadata_insert_query)

    import_records_insert_query = """
            INSERT INTO import.records (id, vendor)
            VALUES
                (1, 'doubleclick');
        """
    connection.execute(import_records_insert_query)

    vendor_ids_maps_insert_query = """
            INSERT INTO vendor_ids.maps (li_code, creative_rtb_id, date_start, date_end, vendor, vendor_id, is_deleted)
            VALUES
                ('LI-123456', 1111111, '2018-04-30', '2018-05-03', 'doubleclick', '12121212', 'f'),
                ('LI-123456', 2222222, '2018-04-30', '2018-05-03', 'doubleclick', '23232323', 'f'),
                ('LI-7891011', 3333333, '2018-04-30', '2018-05-03', 'doubleclick', '34343434', 'f'),
                ('LI-7891011', 4444444, '2018-04-30', '2018-05-03', 'doubleclick', '45454545', 'f'),
                ('LI-7891011', 5555555, '2018-04-30', '2018-05-03', 'doubleclick', '56565656', 'f');
        """
    connection.execute(vendor_ids_maps_insert_query)

    static_calendar_insert_query = """INSERT INTO static.calendar (select i::date from generate_series('2018-04-01', '2018-05-31', '1 day'::interval) i);"""
    connection.execute(static_calendar_insert_query)


def truncate_all_tables(connection):
    connection.execute("TRUNCATE double_click.raw_delivery;")
    connection.execute("TRUNCATE double_click.import_metadata;")
    connection.execute("TRUNCATE import.records CASCADE;")
    connection.execute("TRUNCATE vendor_ids.maps;")
    connection.execute("TRUNCATE vendor_ids.alignment_conflicts;")
    connection.execute("TRUNCATE static.calendar;")


def truncate_output_table(connection):
    connection.execute("TRUNCATE {};".format(OUTPUT_TABLE_FULL_NAME))


def insert_standard_output_data(connection):
    insert_output_query = """
            INSERT INTO {} ("date", flight_id, creative_id, impressions, clicks, provider, time_zone, is_deleted) 
            VALUES  
                    ('2018-05-03', '123456', '1111111', 50714, 7, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-05-03', '123456', '2222222', 1044, 0, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-05-02', '123456', '1111111', 58977, 3, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-05-02', '123456', '2222222', 905, 2, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-05-01', '123456', '1111111', 42303, 3, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-05-01', '123456', '2222222', 2736, 2, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-04-30', '123456', '1111111', 43841, 4, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-04-30', '123456', '2222222', 2941, 2, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-04-30', '7891011', '3333333', 1809, 2, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-04-30', '7891011', '4444444', 19032, 4, 'doubleclick', 'America/New_York', 'f'),
                    ('2018-04-30', '7891011', '5555555', 5588, 1, 'doubleclick', 'America/New_York', 'f');
        """.format(OUTPUT_TABLE_FULL_NAME)
    connection.execute(insert_output_query)


def select_all_from_output_table(connection):
    return {tuple(rowproxy.values()) for rowproxy in connection.execute("select date, flight_id, creative_id, impressions, clicks, provider, time_zone, is_deleted from {} ".format(OUTPUT_TABLE_FULL_NAME)).fetchall()}


def get_standard_output_data():
    return get_standard_output_data_flight123456().union(get_standard_output_data_flight7891011())


def get_standard_output_data_flight7891011():
    return {
        (datetime.date(2018, 4, 30), '7891011', '3333333', 1809, 2, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 4, 30), '7891011', '4444444', 19032, 4, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 4, 30), '7891011', '5555555', 5588, 1, 'doubleclick', 'America/New_York', False)
    }


def get_standard_output_data_flight123456():
    return {
        (datetime.date(2018, 5, 3), '123456', '1111111', 50714, 7, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 5, 3), '123456', '2222222', 1044, 0, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 5, 2), '123456', '1111111', 58977, 3, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 5, 2), '123456', '2222222', 905, 2, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 5, 1), '123456', '1111111', 42303, 3, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 5, 1), '123456', '2222222', 2736, 2, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 4, 30), '123456', '1111111', 43841, 4, 'doubleclick', 'America/New_York', False),
        (datetime.date(2018, 4, 30), '123456', '2222222', 2941, 2, 'doubleclick', 'America/New_York', False)
    }
