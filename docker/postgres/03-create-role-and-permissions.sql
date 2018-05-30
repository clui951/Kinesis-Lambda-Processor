GRANT USAGE ON SCHEMA double_click TO db_username;
GRANT USAGE ON SCHEMA vendor_ids TO db_username;
GRANT USAGE ON SCHEMA import TO db_username;
GRANT USAGE ON SCHEMA snoopy TO db_username;
GRANT ALL ON snoopy.delivery_by_flight_creative_day TO db_username;
GRANT SELECT ON double_click.raw_delivery TO db_username;
GRANT SELECT ON double_click.import_metadata TO db_username;
GRANT SELECT ON import.records TO db_username;
GRANT SELECT ON vendor_ids.maps TO db_username;
GRANT SELECT ON vendor_ids.alignment_conflicts TO db_username;