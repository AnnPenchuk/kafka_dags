-- Create owner
CREATE USER admin WITH PASSWORD 'password';

-- Create db - source_template
CREATE DATABASE source_template
  WITH 
  OWNER = admin
  ENCODING = 'UTF8'
  TEMPLATE template1;

UPDATE pg_database SET datistemplate = TRUE WHERE datname = 'source_template';

ALTER DATABASE source_template WITH ALLOW_CONNECTIONS = false;

-- Create db - source_prod
CREATE DATABASE source_prod
  WITH 
  OWNER = admin
  ENCODING = 'UTF8'
  TEMPLATE source_template;

-- Create db - source_dev
CREATE DATABASE source_dev
  WITH 
  OWNER = admin
  ENCODING = 'UTF8'
  TEMPLATE source_template;
