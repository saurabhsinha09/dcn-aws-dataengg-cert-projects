create schema processed_zone;

CREATE TABLE processed_zone.fact_apartment_viewings (
    viewing_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    apartment_id BIGINT NOT NULL ,
    user_id INT NOT NULL,
    viewed_at TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50),
    price DECIMAL(10, 2),
    fee DECIMAL(10, 2),
    currency VARCHAR(10)
);

CREATE TABLE processed_zone.dim_apartments (
    apartment_id BIGINT PRIMARY KEY,
    title VARCHAR(255),
    category VARCHAR(255),
    body VARCHAR(2000),
    amenities TEXT,
    bedrooms DECIMAL(3,1),
    bathrooms DECIMAL(3,1),
    square_feet INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(50),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    has_photo VARCHAR(10),
    pets_allowed VARCHAR(255),
    price_display VARCHAR(255),
    price_type VARCHAR(50)
);

CREATE TABLE processed_zone.dim_users (
    user_id INT PRIMARY KEY
);
