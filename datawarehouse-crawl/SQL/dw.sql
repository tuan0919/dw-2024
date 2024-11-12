	create database if not exists datawarehouse;
	use datawarehouse;
    
	ALTER DATABASE datawarehouse
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_unicode_ci;


	DROP TABLE IF EXISTS product_dim;
	-- Tạo bảng product_dim để lưu trữ dữ liệu sản phẩm
	CREATE TABLE IF NOT EXISTS product_dim (
		id INT AUTO_INCREMENT PRIMARY KEY,
		product_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        price DECIMAL(18,2),
		image MEDIUMTEXT,
		size VARCHAR(255),
		weight VARCHAR(50),
		resolution VARCHAR(255),
		sensor VARCHAR(255),
		connectivity VARCHAR(255),
		battery VARCHAR(255),
		compatibility VARCHAR(255),
		manufacturer VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		isDelete BOOLEAN DEFAULT FALSE, -- Cột kiểm tra trạng thái xóa
		date_delete DATE,                -- Ngày xóa
		date_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Ngày chèn
		expired_date DATE DEFAULT '9999-12-31' -- Ngày hết hạn mặc định
	);

	-- tạo bảng date_dim

	CREATE TABLE IF NOT EXISTS date_dim (
		date_sk INT PRIMARY KEY,
		full_date DATE NOT NULL,
		day_since_2005 INT NOT NULL,
		month_since_2005 INT NOT NULL,
		day_of_week VARCHAR(10) NOT NULL,
		calendar_month VARCHAR(10) NOT NULL,
		calendar_year INT NOT NULL,
		calendar_year_month VARCHAR(10) NOT NULL, -- Đã điều chỉnh để đủ 8 ký tự
		day_of_month INT NOT NULL,
		day_of_year INT NOT NULL,
		week_of_year_sunday INT NOT NULL,
		year_week_sunday VARCHAR(10) NOT NULL,
		week_sunday_start DATE NOT NULL,
		week_of_year_monday INT NOT NULL,
		year_week_monday VARCHAR(10) NOT NULL,
		week_monday_start DATE NOT NULL,
		holiday VARCHAR(15) NOT NULL,
		day_type VARCHAR(10) NOT NULL
	);
ALTER TABLE product_dim
    DROP COLUMN size,
    ADD COLUMN length DECIMAL(18,2),
    ADD COLUMN width DECIMAL(18,2),
    ADD COLUMN height DECIMAL(18,2);

ALTER TABLE product_dim
	ADD COLUMN source varchar(255);
ALTER TABLE product_dim
	ADD COLUMN date_insert_fk int;
ALTER TABLE product_dim
    ADD CONSTRAINT fk_date_insert FOREIGN KEY (date_insert_fk) REFERENCES date_dim(date_sk);

