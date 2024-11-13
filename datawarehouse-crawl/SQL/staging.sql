	CREATE DATABASE IF NOT EXISTS dbstaging CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
	USE dbstaging;

	 -- Set kiểu chữ cho các thuộc tính trong dbstaging
	ALTER DATABASE dbstaging
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_unicode_ci;
	/*
	Vì đảm bảo quá trình load vào staging không bị lôi, đưa tất cả các cột của bảng tạm thành kiểu dữ liệu text
	Sau đó khi load vào dbstaging sẽ tiến hành transform và cleaning dữ liệu 
	Sau khi cleaning và transform dữ liệu sẽ được lưu vào staging
	Khi bảng staging có dữ liệu, sẽ có 1 proc để load từ staging vào datawarehouse 
	*/
	-- tạo bảng tạm cho nguồn gearvn
    
	CREATE TABLE if not exists staging_gearvn (
		id INT AUTO_INCREMENT PRIMARY KEY,
		weight TEXT,
		brand TEXT,
		dpi TEXT,                -- giữ nguyên tên cột là dpi
		size TEXT,
		sensor TEXT,          -- giữ nguyên tên cột là connector
		connector TEXT,                -- giữ nguyên tên cột là pin
		os TEXT,                 -- giữ nguyên tên cột là os
		pin TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, -- giữ nguyên tên cột là brand
		images TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,  -- giữ nguyên tên cột là name
		name TEXT,
		price TEXT,             -- giữ nguyên tên cột là images
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	-- tạo bảng tạm cho nguồn cellphones
	CREATE TABLE if not exists staging_mouse_cellphones (
		id INT AUTO_INCREMENT PRIMARY KEY,
		weight TEXT,
		size TEXT,
		dpi TEXT,                -- giữ nguyên tên cột là dpi
		sensor TEXT,
		connector TEXT,          -- giữ nguyên tên cột là connector
		pin TEXT,                -- giữ nguyên tên cột là pin
		os TEXT,                 -- giữ nguyên tên cột là os
		brand TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, -- giữ nguyên tên cột là brand
		name TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,  -- giữ nguyên tên cột là name
		price TEXT,
		images TEXT,             -- giữ nguyên tên cột là images
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);




	-- Tạo bảng staging để lưu dữ liệu từ 2 bảng tạm
	CREATE TABLE if not exists staging_mouse_daily (
		id INT AUTO_INCREMENT PRIMARY KEY,
		product_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        price DECIMAL(18,2),
		image MEDIUMTEXT,
		size VARCHAR(255),
		weight decimal(18,2),
		resolution VARCHAR(255),
		sensor VARCHAR(255),
		connectivity VARCHAR(255),
		battery VARCHAR(255),
		compatibility VARCHAR(255),
		manufacturer VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
    
    CREATE TABLE if not exists staging_mouse_daily_gearvn (
		id INT AUTO_INCREMENT PRIMARY KEY,
		product_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        price DECIMAL(18,2),
		image MEDIUMTEXT,
		size VARCHAR(255),
		weight decimal(18,2),
		resolution VARCHAR(255),
		sensor VARCHAR(255),
		connectivity VARCHAR(255),
		battery VARCHAR(255),
		compatibility VARCHAR(255),
		manufacturer VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);


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
    
ALTER TABLE dbstaging.staging_mouse_daily
    DROP COLUMN size,
    ADD COLUMN length DECIMAL(18,2),
    ADD COLUMN width DECIMAL(18,2),
    ADD COLUMN height DECIMAL(18,2);
alter table dbstaging.staging_mouse_daily
	ADD COLUMN source varchar(255);
    
    ALTER TABLE dbstaging.staging_mouse_daily_gearvn
    DROP COLUMN size,
    ADD COLUMN length DECIMAL(18,2),
    ADD COLUMN width DECIMAL(18,2),
    ADD COLUMN height DECIMAL(18,2);
alter table dbstaging.staging_mouse_daily_gearvn
	ADD COLUMN source varchar(255)


