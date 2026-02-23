CREATE TABLE rides (
    ride_id SERIAL PRIMARY KEY,
    driver_id INT,
    city VARCHAR(50),
    timestamp TIMESTAMP,
    price NUMERIC(5,2),
    status VARCHAR(20)
);

ALTER TABLE rides REPLICA IDENTITY FULL;

-- Insert sample rows 
INSERT INTO rides (driver_id, city, timestamp, price, status)
VALUES
(101, 'Warsaw', NOW(), 25.50, 'completed'),
(102, 'Warsaw', NOW(), 12.75, 'completed'),
(103, 'Warsaw', NOW(), 18.00, 'cancelled');
