# Sử dụng PostgreSQL image chính thức từ Docker Hub
FROM postgres:15

# Thiết lập các biến môi trường cho PostgreSQL
# (Tạo database và user mặc định khi container chạy lần đầu)
ENV POSTGRES_USER=my_user
ENV POSTGRES_PASSWORD=my_password
ENV POSTGRES_DB=my_database

# Sao chép các tệp cấu hình hoặc script SQL nếu cần
# Ví dụ: script để khởi tạo database schema hoặc seed data
COPY ./postgres_config/init.sql /docker-entrypoint-initdb.d/

# Expose cổng mặc định của PostgreSQL
EXPOSE 5432

# Sử dụng entrypoint mặc định của PostgreSQL
CMD ["postgres"]
