CREATE USER backend_app WITH PASSWORD 'password_backend_app';
GRANT travel_rw TO backend_app;

CREATE USER developer WITH PASSWORD 'password_developer';
GRANT travel_r TO developer;

CREATE USER analyst WITH PASSWORD 'password_analyst';
GRANT travel_r TO analyst;
