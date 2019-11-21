-- Create tables

CREATE TABLE organization_table (
  id SERIAL NOT NULL PRIMARY KEY,
  stable_id text NOT NULL,
  name text,
  description text,
  address text,
  welcome_url text,
  contact_url text,
  logo_url text,
  info text
);

CREATE TABLE service_table (
  id SERIAL NOT NULL PRIMARY KEY,
  stable_id text NOT NULL,
  name text NOT NULL,
  service_type text NOT NULL,
  api_version text NOT NULL,
  service_url text NOT NULL,
  entry_point boolean NOT NULL,
  organization_id INT REFERENCES organization_table (id) NOT NULL,
  description text,
  version text,
  open boolean NOT NULL,
  welcome_url text,
  alternative_url  text,
  create_date_time timestamp(6) without time zone,
  update_date_time timestamp(6) without time zone
);


-- Insert mock data

INSERT INTO organization_table (stable_id, name, description, address, welcome_url, contact_url, logo_url, info) VALUES ('org.example', 'Org-Example', 'This is an example', '123 Street', 'welcome.com', 'contact@me', 'logo.com', 'extra_info');
INSERT INTO organization_table (stable_id, name, description, address, welcome_url, contact_url, logo_url, info) VALUES ('org.example2', 'Org-Example2', 'This is an example2', '321 Street', 'welcome2.com', 'contact2@me', 'logo2.com', 'extra_info2');


INSERT INTO service_table (stable_id, name, service_type, api_version, service_url, entry_point, organization_id, description, version, open, welcome_url, alternative_url, create_date_time, update_date_time) VALUES ('BA1', 'BA1', 'GA4GHBeaconAggregator', 'v1', 'BA1.com', true, '1', 'BA1 description', 'v2', true, 'BA1-welcome.com', 'BA1-alternative.com', '2019-09-26', '2019-09-26');
INSERT INTO service_table (stable_id, name, service_type, api_version, service_url, entry_point, organization_id, description, version, open, welcome_url, alternative_url, create_date_time, update_date_time) VALUES ('BA2', 'BA2', 'GA4GHBeaconAggregator', 'v1', 'BA2.com', true, '2', 'BA2 description', 'v2', true, 'BA2-welcome.com', 'BA2-alternative.com', '2019-09-26', '2019-09-26');
INSERT INTO service_table (stable_id, name, service_type, api_version, service_url, entry_point, organization_id, description, version, open, welcome_url, alternative_url, create_date_time, update_date_time) VALUES ('R1', 'R1', 'GA4GHRegistry', 'v1', 'R1.com', false, '1', 'R1 description', 'v2', true, 'R1-welcome.com', 'R1-alternative.com', '2019-09-26', '2019-09-26');


-- Create view
CREATE VIEW service AS
SELECT 
  s.id,
  s.stable_id as service_stable_id,
  s.name as service_name,
  s.service_type,
  s.api_version,
  s.service_url,
  s.entry_point,
  s.description as service_description,
  s.version,
  s.open,
  s.welcome_url as service_welcome_url,
  s.alternative_url,
  s.create_date_time,
  s.update_date_time,
  o.id as organization_id,
  o.stable_id as organization_stable_id,
  o.name as organization_name,
  o.description as organization_description,
  o.address,
  o.welcome_url as organization_welcome_url,
  o.contact_url,
  o.logo_url,
  o.info
  FROM service_table s
  JOIN organization_table o ON s.organization_id=o.id;

