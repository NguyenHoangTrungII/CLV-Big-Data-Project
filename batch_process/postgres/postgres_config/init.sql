-- Tạo bảng ví dụ
-- CREATE TABLE IF NOT EXISTS users (
--     id SERIAL PRIMARY KEY,
--     name VARCHAR(100),
--     email VARCHAR(100) UNIQUE NOT NULL
-- );

-- CREATE TABLE log (
--     id SERIAL PRIMARY KEY,
--     dttm TIMESTAMPTZ,
--     event TEXT,
--     owner TEXT,
--     extra JSONB
-- );


-- -- Seed dữ liệu ví dụ
-- INSERT INTO users (name, email) VALUES
-- ('Alice', 'alice@example.com'),
-- ('Bob', 'bob@example.com');


CREATE TABLE sales_data (
    Country TEXT,
    CustomerID FLOAT,
    Description TEXT,
    InvoiceDate TIMESTAMP,
    InvoiceNo TEXT,
    Quantity INTEGER,
    StockCode TEXT,
    UnitPrice FLOAT,
    Revenue FLOAT,
    CLV_Prediction FLOAT
);


CREATE TABLE sales_data_order(
    Country VARCHAR(255),
    CustomerID VARCHAR(255),
    Description TEXT,
    InvoiceDate TIMESTAMP,
    InvoiceNo VARCHAR(255),
    Quantity INT,
    StockCode VARCHAR(255),
    UnitPrice FLOAT,
    Revenue FLOAT,
    CLV_Prediction DOUBLE PRECISION NOT NULL
);

