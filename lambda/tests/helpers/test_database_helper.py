import pytest
import traceback
import datetime

from sqlalchemy.exc import OperationalError

from config import db_config
from helpers import database_helper as h

OUTPUT_TABLE_FULL_NAME = h.OUTPUT_SCHEMA + "." + h.OUTPUT_TABLE


@pytest.fixture(scope="module")
def engine():
	db_endpoint = db_config.db_endpoint
	db_username = db_config.db_username
	db_password = db_config.db_password
	db_name = db_config.db_name
	db_postgres_string = "postgres://" + db_username + ":" + db_password + "@" + db_endpoint + "/" + db_name
	return h.create_new_engine(db_postgres_string)

@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown_for_entire_test_suite(engine):
	# Setup before starting entire test suite
	truncate_all_tables(engine.connect())
	load_upstream_table_data(engine.connect())
	yield
	# Teardown after ending entire test suite
	truncate_all_tables(engine.connect())

@pytest.fixture(scope="function")
def connection(engine):
	return engine.connect()

@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown_for_each_test(connection):
	# Setup before each test
	insert_standard_output_data(connection)
	yield
	# Teardown after each test
	truncate_output_table(connection)

def test_process_processing_id_flight_id_with_empty_table_populate_expected_output(connection):
	results = select_all_from_output_table(connection)
	assert get_standard_output_data() == results

def test_process_processing_id_import_id_with_empty_table_populate_expected_output(connection):
	assert True

def test_process_processing_id_flight_id_with_deleted_rows_populate_marks_as_deleted(connection):
	assert True

def test_process_processing_id_flight_id_with_undeleted_rows_populate_marks_as_not_deleted(connection):
	assert True

def test_process_processing_id_flight_id_with_updated_data_populate_updates_data(connection):
	assert True

def test_process_processing_id_flight_id_with_new_data_populate_new_data(connection):
	assert True

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

def test_generate_expected_data_temp_table_processing_id_creates_correct_temp_table(connection):
	assert True

def test_calculate_diffs_and_writes_to_output_table_temp_table_returns_correct_diffs(connection):
	assert True

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
				(2, 34343434, '2018-04-30', 1809, 2, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
				(2, 45454545, '2018-04-30', 19032, 4, 000, 000, 'junk', 000, 'junk', 000, 'junk'),
				(2, 56565656, '2018-04-30', 5588, 1, 000, 000, 'junk', 000, 'junk', 000, 'junk');
		"""
	connection.execute(doubleclick_raw_delivery_insert_query)

	doubleclick_import_metadata_insert_query = """
			INSERT INTO double_click.import_metadata (import_record_id, report_time_zone, s3_path, credential, profile_id)
			VALUES
				(1, 'Australia/Sydney', 'junk', 'junk', 0),
				(2, 'America/New_York', 'junk', 'junk', 0);
		"""
	connection.execute(doubleclick_import_metadata_insert_query)

	import_records_insert_query = """
			INSERT INTO import.records (id, vendor)
			VALUES
				(1, 'DoubleClick'),
				(2, 'DoubleClick');
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

def truncate_all_tables(connection):
	connection.execute("TRUNCATE double_click.raw_delivery;")
	connection.execute("TRUNCATE double_click.import_metadata;")
	connection.execute("TRUNCATE import.records CASCADE;")
	connection.execute("TRUNCATE vendor_ids.maps;")
	truncate_output_table(connection)

def truncate_output_table(connection):
	connection.execute("TRUNCATE {};".format(OUTPUT_TABLE_FULL_NAME))		

def insert_standard_output_data(connection):
	insert_output_query = """
			INSERT INTO {} ("date", flight_id, creative_id, impressions, clicks, provider, time_zone, updated_at, is_deleted) 
			VALUES 	
					('2018-05-03', '123456', '1111111', 50714, 7, 'DoubleClick', 'Australia/Sydney', '2018-05-22 10:30:00.0', 'f'),
					('2018-05-03', '123456', '2222222', 1044, 0, 'DoubleClick', 'Australia/Sydney', '2018-05-22 10:30:00.0', 'f'),
					('2018-05-02', '123456', '1111111', 58977, 3, 'DoubleClick', 'Australia/Sydney', '2018-05-22 10:30:00.0', 'f'),
					('2018-05-02', '123456', '2222222', 905, 2, 'DoubleClick', 'Australia/Sydney', '2018-05-22 10:30:00.0', 'f'),
					('2018-05-01', '123456', '1111111', 42303, 3, 'DoubleClick', 'Australia/Sydney', '2018-05-22 10:30:00.0', 'f'),
					('2018-05-01', '123456', '2222222', 2736, 2, 'DoubleClick', 'Australia/Sydney', '2018-05-22 10:30:00.0', 'f'),
					('2018-04-30', '123456', '1111111', 43841, 4, 'DoubleClick', 'Australia/Sydney', '2018-05-22 10:30:00.0', 'f'),
					('2018-04-30', '123456', '2222222', 2941, 2, 'DoubleClick', 'Australia/Sydney', '2018-05-22 10:30:00.0', 'f'),
					('2018-04-30', '7891011', '3333333', 1809, 2, 'DoubleClick', 'America/New_York', '2018-05-22 11:45:0.0', 'f'),
					('2018-04-30', '7891011', '4444444', 19032, 4, 'DoubleClick', 'America/New_York', '2018-05-22 11:45:0.0', 'f'),
					('2018-04-30', '7891011', '5555555', 5588, 1, 'DoubleClick', 'America/New_York', '2018-05-22 11:45:0.0', 'f');
		""".format(OUTPUT_TABLE_FULL_NAME)
	connection.execute(insert_output_query)

def select_all_from_output_table(connection):
	return connection.execute("select * from {} ".format(OUTPUT_TABLE_FULL_NAME)).fetchall()

def get_standard_output_data():
	return [
		(datetime.date(2018, 5, 3), '123456', '1111111', 50714, 7, 'DoubleClick', 'Australia/Sydney', datetime.datetime(2018, 5, 22, 10, 30, 00, 0), False),
		(datetime.date(2018, 5, 3), '123456', '2222222', 1044, 0, 'DoubleClick', 'Australia/Sydney', datetime.datetime(2018, 5, 22, 10, 30, 00, 0), False),
		(datetime.date(2018, 5, 2), '123456', '1111111', 58977, 3, 'DoubleClick', 'Australia/Sydney', datetime.datetime(2018, 5, 22, 10, 30, 00, 0), False),
		(datetime.date(2018, 5, 2), '123456', '2222222', 905, 2, 'DoubleClick', 'Australia/Sydney', datetime.datetime(2018, 5, 22, 10, 30, 00, 0), False),
		(datetime.date(2018, 5, 1), '123456', '1111111', 42303, 3, 'DoubleClick', 'Australia/Sydney', datetime.datetime(2018, 5, 22, 10, 30, 00, 0), False),
		(datetime.date(2018, 5, 1), '123456', '2222222', 2736, 2, 'DoubleClick', 'Australia/Sydney', datetime.datetime(2018, 5, 22, 10, 30, 00, 0), False),
		(datetime.date(2018, 4, 30), '123456', '1111111', 43841, 4, 'DoubleClick', 'Australia/Sydney', datetime.datetime(2018, 5, 22, 10, 30, 00, 0), False),
		(datetime.date(2018, 4, 30), '123456', '2222222', 2941, 2, 'DoubleClick', 'Australia/Sydney', datetime.datetime(2018, 5, 22, 10, 30, 00, 0), False),
		(datetime.date(2018, 4, 30), '7891011', '3333333', 1809, 2, 'DoubleClick', 'America/New_York', datetime.datetime(2018, 5, 22, 11, 45, 00, 0), False),
		(datetime.date(2018, 4, 30), '7891011', '4444444', 19032, 4, 'DoubleClick', 'America/New_York', datetime.datetime(2018, 5, 22, 11, 45, 00, 0), False),
		(datetime.date(2018, 4, 30), '7891011', '5555555', 5588, 1, 'DoubleClick', 'America/New_York', datetime.datetime(2018, 5, 22, 11, 45, 00, 0), False)
	]
