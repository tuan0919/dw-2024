use dbstaging;

/*QUERY LOAD DATA INFILE
LẤY FILE CÓ TRẠNG THÁI LÀ L_RE (LOAD READY EXTRACT) ĐỂ ĐƯA VÀO CÂU LỆNH LOAD DATA IFILE
MỤC ĐÍCH CỦA PROCEDURE LÀ LẤY ĐƯỢC QUERY LOAD CSV VÀO STAGING ĐỂ EXEC Ở JAVA
*/

delimiter //
create procedure load_csv_to_temp_staging()
begin
	declare file_paths varchar(255);
    declare fields_terminated varchar(10);
	declare optionally_enclosed varchar(10);
	declare lines_terminated	varchar(10);
	declare ignore_row	int;
    declare table_staging varchar(50);
    DECLARE stg_fields text;
    declare log_id int;
    
    -- lấy các thuộc tính cần thiết để điền vào query load data infile
    select fl.file_path, cf.fields_terminated_by, cf.optionally_enclosed_by, cf.lines_terminated_by, cf.ignore_rows, cf.staging_fields, fl.file_log_id, cf.staging_table
    into file_paths,fields_terminated, optionally_enclosed, lines_terminated, ignore_row, stg_fields, log_id, table_staging
    from dbcontrol.file_logs fl join dbcontrol.configs cf
    on fl.config_id = cf.config_id
    where fl.status = 'C_SE'
    limit 1;

    -- Kiểm tra xem file có null hay không
    if file_paths is null then
		signal sqlstate '45000'
        set message_text = 'Error: File path is NULL or no file with READY EXTRACT status!';
	end if;
    
    -- Tạo load data infile động
    SET @sql = CONCAT(
        "LOAD DATA INFILE '", file_paths, "' ",
        "INTO TABLE dbstaging.", table_staging, " ", -- 
        "FIELDS TERMINATED BY '", fields_terminated, "' ", --
        "OPTIONALLY ENCLOSED BY '", optionally_enclosed, "' ", --
        "LINES TERMINATED BY '", lines_terminated, "' ", -- 
        "IGNORE ", ignore_row, " ROWS ", --
        "(", stg_fields, ");" -- Sử dụng các trường trong staging_fields
    );
    
    SELECT @sql AS debug_query, log_id AS file_log_id;
    
end //
delimiter ;
CALL load_csv_to_temp_staging();

/*
UPDATE STATUS IN DBCONTROL.FILE_LOGS
*/
DELIMITER //

CREATE PROCEDURE update_file_status(
    IN log_id INT,
    IN new_status VARCHAR(20),
    IN start_time DATETIME,
    IN end_time DATETIME
)
BEGIN
    -- Cập nhật trạng thái và thời gian vào bảng file_logs
    UPDATE dbcontrol.file_logs
    SET 
        status = new_status,
        start_time = start_time,
        end_time = end_time
    WHERE 
        file_log_id = log_id;
END //

DELIMITER ;


/*TRANSFORM + CLEANING DỮ LIỆU*/

delimiter //
create procedure transform_and_cleaning_data()
begin
	-- TRANSFROM 
    -- Với riêng bài của nhóm thì không có xử lý nhiều vì các thuộc tính không lưu được dưới dạng khác ngoài chuỗi
    -- Nếu có khác kiểu dữ liệu giữa bảng tạm và bảng chính thì khi load phải tiến hành sử dụng hàm Cast hoặc convert kiểu dữ liệu
    /*
    Cụ thể:
		chuyển sang int: CAST(thuoc_tinh AS INT)
        chuyển sang datetime: STR_TO_DATE(created_at, '%Y-%m-%d %H:%i:%s')
        thắc mắc gì thì hỏi thêm
    */
    -- tạo bảng tạm để load dữ liệu từ các nguồn khác nhau, cụ thể ở đây là 2 nguồn. Bảng này có cấu trúc giống hoàn toàn với staging_main
    DROP TEMPORARY TABLE IF EXISTS staging_combined;
	DROP TEMPORARY TABLE IF EXISTS temp_product;
    
    CREATE TEMPORARY TABLE staging_combined (
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
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    -- insert bảng staging_gearvn vào bảng tạm
    /*
    INSERT INTO staging_combined  (
        product_name,
        image,
        size,
        weight,
        resolution,
        sensor,
        button,
        connectivity,
        battery,
        compatibility,
        utility,
        manufacturer,
        created_at
    )
    SELECT 
        product_name,
        image,
        size,
        weight,
        resolution,
        sensor,
        button,
        connectivity,
        battery,
        compatibility,
        utility,
        manufacturer,
        created_at
    FROM 
        staging_gearvn; */
	SET SESSION group_concat_max_len = 1000000;  -- Hoặc giá trị lớn hơn tùy theo nhu cầu

    -- insert bảng staging_cellphones vào bảng tạm
	INSERT INTO staging_combined (
		product_name,
		price,
		image,
		size,
		weight,
		resolution,
		sensor,
		connectivity,
		battery,
		compatibility,
		manufacturer,
		created_at
	)
	SELECT 
		name,
		CASE 
			WHEN price = 'Giá Liên Hệ' THEN NULL  -- Nếu giá là "Giá Liên Hệ", đặt thành NULL
			WHEN price IS NULL OR price = '' THEN NULL  -- Trường hợp giá rỗng hoặc NULL
			WHEN price REGEXP '^[0-9]+(\\.[0-9]{1,3})?đ$' THEN  -- Kiểm tra định dạng số kèm "đ"
				CAST(REPLACE(REPLACE(price, 'đ', ''), '.', '') AS DECIMAL(18,2))  -- Chuyển đổi thành DECIMAL
			ELSE 
				NULL  -- Các trường hợp không xác định, đặt thành NULL
		END AS price,
		(
        SELECT GROUP_CONCAT(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(image, 'plain/', -1), ',', 1)) SEPARATOR ', ')
			FROM (
				SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(images, 'plain/', -1), ',', n.n)) AS image
				FROM staging_mouse_cellphones
				JOIN (
					SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
					UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
					UNION ALL SELECT 9 UNION ALL SELECT 10
				) n ON CHAR_LENGTH(images) - CHAR_LENGTH(REPLACE(images, ',', '')) >= n.n - 1
				WHERE images IS NOT NULL
			) AS temp
		) AS image,
		size,
		weight,
		dpi,
		sensor,
		connector,
		pin,
		os,
		brand,
		created_at
	FROM 
		staging_mouse_cellphones;

    
    -- CLEANING
    /*
    1. Kiểm tra xem có bị trùng dữ liệu hay không bằng cách so sánh nk (có thì so sánh)
    2. Xác định các trường dữ liệu quan trọng, tiến hành tiền xử lý dữ liệu tùy theo ý
    */
    -- Check trùng
    -- tiếp tục tạo thêm 1 bảng tạm để lưu các dòng dữ liệu không bị trùng
    CREATE TEMPORARY TABLE temp_product AS
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_name, manufacturer, size, weight, resolution ORDER BY manufacturer) AS row_num
        FROM dbstaging.staging_combined
    ) AS temp
    WHERE row_num = 1;
    -- chuyển mã cho chắc :v
    ALTER TABLE temp_product
	CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    
    -- Tiến hành insert dữ liệu đã được xử lý vào staging
    INSERT INTO staging_mouse_daily  (
        product_name,
		price,
		image,
		size,
		weight,
		resolution,
		sensor,
		connectivity,
		battery,
		compatibility,
		manufacturer,
		created_at
    )
    SELECT 
        product_name,
		price,
		image,
		size,
		weight,
		resolution,
		sensor,
		connectivity,
		battery,
		compatibility,
		manufacturer,
		created_at
    FROM 
        temp_product;
    
    
end //
delimiter ;
call transform_and_cleaning_data;

