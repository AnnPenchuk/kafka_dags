-- Create default role - read/write 
CREATE ROLE sandbox_rw;

GRANT ALL PRIVILEGES ON SCHEMA sandbox TO sandbox_rw;

ALTER DEFAULT PRIVILEGES IN SCHEMA sandbox
GRANT ALL PRIVILEGES ON TABLES TO sandbox_rw;


-- Create default user
CREATE USER sandbox_user WITH PASSWORD 'password';
GRANT sandbox_rw TO sandbox_user;
