CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE SCHEMA util;
CREATE TYPE util.import_status AS ENUM('IN PROGRESS','COMPLETE','FAILED');


CREATE SCHEMA static;
CREATE TABLE static.calendar (
	report_date date NOT NULL,
	PRIMARY KEY (report_date)
);

-- double_click.raw_delivery
CREATE TABLE double_click.raw_delivery (
	import_record_id int4 NOT NULL,
	advertiser text NOT NULL,
	advertiser_id int4 NOT NULL,
	campaign text NOT NULL,
	campaign_id int4 NOT NULL,
	campaign_start_date date,
	campaign_end_date date,
	placement text,
	placement_id int4 NOT NULL,
	placement_start_date date,
	placement_end_date date,
	placement_total_booked_units int4,
	placement_rate numeric NOT NULL,
	placement_cost_structure text,
	ad text,
	ad_id int4 NOT NULL,
	ad_status text,
	site_keyname text NOT NULL,
	"date" date NOT NULL,
	impressions int4 DEFAULT 0,
	clicks int4 DEFAULT 0,
	downloaded_impressions int8,
	PRIMARY KEY (ad_id,placement_id,campaign_id,"date")
);
CREATE INDEX dfa_raw_delivery_site_keyname_date ON double_click.raw_delivery USING btree (site_keyname, date);
CREATE INDEX raw_delivery_import_id_key ON double_click.raw_delivery USING btree (import_record_id);
CREATE INDEX raw_delivery_placement_id_key ON double_click.raw_delivery USING btree (((placement_id)::text));

-- double_click.import_metadata
CREATE TABLE double_click.import_metadata (
	import_record_id serial NOT NULL,
	s3_path text NOT NULL,
	credential text NOT NULL,
	profile_id int4 NOT NULL,
	imported_at timestamp,
	date_time_generated text,
	report_time_zone text,
	subaccount_name text,
	account_id int4,
	start_date text,
	end_date text,
	s3_updated_at timestamp
);
CREATE INDEX import_metadata_key ON double_click.import_metadata USING btree (import_record_id);

-- vendor_ids.maps
CREATE TABLE vendor_ids.maps (
	li_code text NOT NULL,
	creative_rtb_id int4 NOT NULL,
	date_start date NOT NULL,
	date_end date NOT NULL,
	vendor text NOT NULL,
	vendor_id text NOT NULL,
	campaign_rtb_id text,
	is_deleted bool DEFAULT false,
	PRIMARY KEY (vendor_id,vendor,date_end,date_start,creative_rtb_id,li_code)
);
CREATE INDEX maps_vendor_vendor_id_date_start_date_end_idx ON vendor_ids.maps USING btree (vendor, vendor_id, date_start, date_end) WHERE (is_deleted = false);
CREATE INDEX trgm_maps_vendor_id ON vendor_ids.maps USING gin (vendor_id gin_trgm_ops);

-- vendor_ids.alignment_conflicts
CREATE TABLE vendor_ids.alignment_conflicts (
	li_code text,
	li_code_2 text,
	vendor text,
	date_start date,
	date_end date,
	creative_ids _int4,
	vendor_ids _text
);

-- import.records
CREATE TABLE import.records (
	"id" serial NOT NULL,
	vendor text,
	extract_artifact text,
	started_at timestamp DEFAULT now(),
	finished_at timestamp,
	status util.import_status DEFAULT 'IN PROGRESS'::util.import_status,
	metadata jsonb,
	PRIMARY KEY ("id")
);

-- snoopy.delivery_by_flight_creative_day
CREATE TABLE snoopy.delivery_by_flight_creative_day (
	"date" date NOT NULL,
	flight_id text NOT NULL,
	creative_id text,
	impressions int8,
	clicks int8,
	provider text,
	time_zone text,
	updated_at timestamp,
	is_deleted bool
);

CREATE UNIQUE INDEX unique_key_snoopy_delivery_date_flightid_timezone_provider_creativeid ON snoopy.delivery_by_flight_creative_day (date, flight_id, time_zone, provider, creative_id) WHERE creative_id IS NOT NULL;
CREATE UNIQUE INDEX unique_key_snoopy_delivery_date_flightid_timezone_provider ON snoopy.delivery_by_flight_creative_day (date, flight_id, time_zone, provider) WHERE creative_id IS NULL;