import pytest
import traceback

from sqlalchemy.exc import OperationalError

from config import db_config
import helpers.database_helper as h

OUTPUT_TABLE_FULL_NAME = h.OUTPUT_SCHEMA + "." + h.OUTPUT_TABLE

@pytest.fixture(scope="module")
def engine():
	db_endpoint  = db_config.db_endpoint
	db_username = db_config.db_username
	db_password = db_config.db_password
	db_name = db_config.db_name
	db_postgres_string = "postgres://" + db_username + ":" + db_password + "@" + db_endpoint + "/" + db_name
	return h.create_new_engine(db_postgres_string)

@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown_for_test_suite(engine):
	# Setup before starting entire test suite
		# truncate output
		# truncate ALL tables and load upstream tables (except output)
	yield
	# Teardown after ending entire test suite
		# truncate ALL tables (except output)

@pytest.fixture(scope="function")
def connection(engine):
	return engine.connect()

@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown_for_each_test(connection):
	# Setup before each test
		# load standard output
	yield
	# Teardown after each test
	connection.execute("TRUNCATE {};".format(OUTPUT_TABLE_FULL_NAME))


def test_process_processing_id_flight_id_with_empty_table_populate_expected_output(connection):
	assert True

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

	load_standard_data(locking_connection)

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

def load_standard_data(connection):
	insert_output_query = """
		INSERT INTO {} ("date", flight_id, creative_id, impressions, clicks, provider, time_zone, updated_at, is_deleted) 
		VALUES 	
				('2018-05-03', '533970', '1310361', 50714, 7, 'DoubleClick', 'Australia/Sydney', '2018-05-22 20:31:50.641754', 'f'),
				('2018-05-03', '533970', '1310362', 1044, 0, 'DoubleClick', 'Australia/Sydney', '2018-05-22 20:31:50.641754', 'f'),
				('2018-05-02', '533970', '1310361', 58977, 3, 'DoubleClick', 'Australia/Sydney', '2018-05-22 20:31:50.641754', 'f'),
				('2018-05-02', '533970', '1310362', 905, 2, 'DoubleClick', 'Australia/Sydney', '2018-05-22 20:31:50.641754', 'f'),
				('2018-05-01', '533970', '1310361', 42303, 3, 'DoubleClick', 'Australia/Sydney', '2018-05-22 20:31:50.641754', 'f'),
				('2018-05-01', '533970', '1310362', 2736, 2, 'DoubleClick', 'Australia/Sydney', '2018-05-22 20:31:50.641754', 'f'),
				('2018-04-30', '533970', '1310361', 43841, 4, 'DoubleClick', 'Australia/Sydney', '2018-05-22 20:31:50.641754', 'f'),
				('2018-04-30', '533970', '1310362', 2941, 2, 'DoubleClick', 'Australia/Sydney', '2018-05-22 20:31:50.641754', 'f'),
				('2018-04-30', '534729', '1311311', 1809, 2, 'DoubleClick', 'America/New_York', '2018-05-22 20:31:43.193941', 'f'),
				('2018-04-30', '534729', '1311312', 19032, 4, 'DoubleClick', 'America/New_York', '2018-05-22 20:31:43.193941', 'f'),
				('2018-04-30', '534729', '1311313', 5588, 1, 'DoubleClick', 'America/New_York', '2018-05-22 20:31:43.193941', 'f');
		""".format(OUTPUT_TABLE_FULL_NAME)
	connection.execute(insert_output_query)




