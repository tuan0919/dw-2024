	CREATE DATABASE IF NOT EXISTS datamart CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
	USE datamart;

	ALTER DATABASE datamart
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_unicode_ci;
    
	CREATE TABLE IF NOT EXISTS product_dim (
    id INT PRIMARY KEY,
    product_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    price DECIMAL(18,2),
    image MEDIUMTEXT,
    weight VARCHAR(50),
    resolution VARCHAR(255),
    sensor VARCHAR(255),
    connectivity VARCHAR(255),
    battery VARCHAR(255),
    compatibility VARCHAR(255),
    manufacturer VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    length DECIMAL(18,2),
    width DECIMAL(18,2),
    height DECIMAL(18,2)
);
ALTER TABLE datamart.product_dim
DROP PRIMARY KEY;

-- Đặt PRIMARY KEY mới
ALTER TABLE datamart.product_dim
ADD PRIMARY KEY (product_name, manufacturer);
