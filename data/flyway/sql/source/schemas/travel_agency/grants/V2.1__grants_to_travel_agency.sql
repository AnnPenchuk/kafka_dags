-- Create default role - read/write 
CREATE ROLE travel_rw;

GRANT ALL PRIVILEGES ON SCHEMA travel_agency TO travel_rw;

ALTER DEFAULT PRIVILEGES IN SCHEMA travel_agency 
GRANT ALL PRIVILEGES ON TABLES TO travel_rw;

-- Create default role - only read 
CREATE ROLE travel_r;

GRANT USAGE ON SCHEMA travel_agency TO travel_r;
GRANT SELECT ON ALL TABLES IN SCHEMA travel_agency TO travel_r;

ALTER DEFAULT PRIVILEGES IN SCHEMA travel_agency
GRANT SELECT ON TABLES TO travel_r;

-- Create default user
CREATE USER travel_admin WITH PASSWORD 'password_admin';
GRANT travel_rw TO travel_admin;
