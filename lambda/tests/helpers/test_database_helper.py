import pytest

from config import db_config
import helpers.database_helper as h

OUTPUT_TABLE_FULL_NAME = h.OUTPUT_SCHEMA + "." + h.OUTPUT_TABLE

@pytest.fixture
def connection():
	db_endpoint  = db_config.db_endpoint
	db_username = db_config.db_username
	db_password = db_config.db_password
	db_name = db_config.db_name
	db_postgres_string = "postgres://" + db_username + ":" + db_password + "@" + db_endpoint + "/" + db_name
	engine = h.create_new_engine(db_postgres_string)
	return engine.connect()

@pytest.fixture(autouse=True)
def setup_and_teardown(connection):
	yield
	connection.execute("TRUNCATE {};".format(OUTPUT_TABLE_FULL_NAME))

def load_standard_data(connection):
	pass

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

def test_set_lock_timeout_for_transaction_timeout_with_expected_error(connection):
	assert True

def test_generate_expected_data_temp_table_processing_id_creates_correct_temp_table(connection):
	assert True

def test_calculate_diffs_and_writes_to_output_table_temp_table_returns_correct_diffs(connection):
	assert True