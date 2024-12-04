CREATE USER data_engineer WITH PASSWORD 'password_de';
GRANT travel_r TO data_engineer;

CREATE USER data_analyst WITH PASSWORD 'password_da';
GRANT travel_r TO data_analyst;

CREATE USER etl_bot WITH PASSWORD 'password_etl';
GRANT travel_r TO etl_bot;

CREATE USER dq_bot WITH PASSWORD 'password_dq';
GRANT travel_r TO dq_bot;
