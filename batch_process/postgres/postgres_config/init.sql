-- Tạo bảng ví dụ
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE log (
    id SERIAL PRIMARY KEY,
    dttm TIMESTAMPTZ,
    event TEXT,
    owner TEXT,
    extra JSONB
);


-- Seed dữ liệu ví dụ
INSERT INTO users (name, email) VALUES
('Alice', 'alice@example.com'),
('Bob', 'bob@example.com');
