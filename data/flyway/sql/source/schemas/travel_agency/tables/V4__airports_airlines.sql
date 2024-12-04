-- Create table airlines
CREATE TABLE IF NOT EXISTS travel_agency.airlines (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NULL,
  code CHAR(2) UNIQUE NULL,
  country VARCHAR(50) NULL,
  headquarters VARCHAR(50) NULL,
  website VARCHAR(2048) NULL CHECK (website ~* '^https?://.+'),
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table contact_info
CREATE TABLE IF NOT EXISTS travel_agency.contact_info (
  id SERIAL PRIMARY KEY,
  phone VARCHAR(50) NULL,
  email VARCHAR(100) NULL UNIQUE,
  website VARCHAR(2048) NULL CHECK (website ~* '^https?://.+'),
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table airports
CREATE TABLE IF NOT EXISTS travel_agency.airports (
  id SERIAL PRIMARY KEY,
  name VARCHAR(70) NULL,
  code CHAR(3) UNIQUE NULL,
  city VARCHAR(20) NULL,
  country VARCHAR(20) NULL,
  services VARCHAR(100) NULL,
  id_contact_info INT UNIQUE,
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_contact_info
    FOREIGN KEY (id_contact_info)
    REFERENCES travel_agency.contact_info(id)
    ON DELETE SET NULL
);

-- Create index
CREATE INDEX idx_airlines_code ON travel_agency.airlines (code);
CREATE INDEX idx_airports_code ON travel_agency.airports (code);
CREATE INDEX idx_airports_country ON travel_agency.airports (country);
