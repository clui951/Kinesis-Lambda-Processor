# config file containing credentials for rds postgres instance
import os

db_endpoint = os.getenv('db_endpoint')
if os.getenv('env') == 'production':
	db_endpoint = db_endpoint or "production"
elif os.getenv('env') == 'staging':
	db_endpoint = db_endpoint or "staging"
else:
	db_endpoint = db_endpoint or "localhost"

db_test_endpoint = os.getenv('db_test_endpoint') or "localhost"

db_username = os.getenv('db_username') or "db_username"
db_password = os.getenv('db_password') or "db_password"
db_name = os.getenv('db_name') or "db_name"