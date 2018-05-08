# config file containing credentials for rds postgres instance
import os

db_endpoint = os.getenv('db_endpoint') or "<db_endpoint>"
db_username = os.getenv('db_username') or "<db_username>"
db_password = os.environ('db_password') or "<db_password>"
db_name = os.environ('db_name') or "<db_name>" 
