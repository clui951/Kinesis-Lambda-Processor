# config file containing credentials for rds postgres instance
import os

if os.getenv('env') == 'production':
	db_endpoint = os.getenv('db_endpoint') or ""
else:
	db_endpoint = os.getenv('db_endpoint') or ""

db_username = os.getenv('db_username') or ""
db_password = os.getenv('db_password') or ""
db_name = os.getenv('db_name') or ""