use dbstaging;

/*QUERY LOAD DATA INFILE
LẤY FILE CÓ TRẠNG THÁI LÀ L_RE (LOAD READY EXTRACT) ĐỂ ĐƯA VÀO CÂU LỆNH LOAD DATA IFILE
MỤC ĐÍCH CỦA PROCEDURE LÀ LẤY ĐƯỢC QUERY LOAD CSV VÀO STAGING ĐỂ EXEC Ở JAVA
*/
drop procedure if exists load_csv_to_temp_staging;
delimiter //
create procedure load_csv_to_temp_staging( in date_load_data date)
begin
	declare file_paths varchar(255);
    declare fields_terminated varchar(10);
	declare optionally_enclosed varchar(10);
	declare lines_terminated varchar(10);
	declare ignore_row	int;
    declare table_staging varchar(50);
    DECLARE stg_fields text;
    declare log_id int;
    IF date_load_data IS NULL THEN
        SET date_load_data = CURDATE();
    END IF;
    -- lấy các thuộc tính cần thiết để điền vào query load data infile
    select fl.file_path, cf.fields_terminated_by, cf.optionally_enclosed_by, cf.lines_terminated_by, cf.ignore_rows, pp.header_csv, fl.file_log_id, cf.staging_table
	into file_paths,fields_terminated, optionally_enclosed, lines_terminated, ignore_row, stg_fields, log_id, table_staging
    from dbcontrol.file_logs fl join dbcontrol.configs cf on fl.config_id = cf.config_id
								join dbcontrol.process_properties pp on pp.property_id = cf.property_id
    where fl.status = 'C_SE' AND DATE(fl.update_at) = date_load_data and fl.config_idload_from_staging_to_dw =1
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


/*
UPDATE STATUS IN DBCONTROL.FILE_LOGS
*/
drop PROCEDURE if exists update_file_status;
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






call transform_and_cleaning_data;
/*TRANSFORM + CLEANING DỮ LIỆU*/
drop procedure if exists transform_and_cleaning_data;
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
        
	SET SESSION group_concat_max_len = 1000000;  -- Hoặc giá trị lớn hơn tùy theo nhu cầu

    -- insert bảng staging_cellphones vào bảng tạm
	INSERT INTO staging_mouse_daily (
		product_name,
		price,
		image,
		length,
        width,
        height,
		weight,
		resolution,
		sensor,
		connectivity,
		battery,
		compatibility,
		manufacturer,
		created_at,
        source
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
       CASE
           -- Trường hợp có 3 kích thước x x x cm ở cuối chuỗi, chuyển đổi thành mm
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) cm$' THEN
               CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' ', -1), ',', '.') AS DECIMAL(18,2)) * 10
			WHEN TRIM(size) REGEXP '^Cao: ([0-9.\\,]+) mm x Rộng: ([0-9.\\,]+) mm x Dày: ([0-9.\\,]+) mm$' THEN
              CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Cao: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(TRIM(size), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+)mm$' THEN
               CAST(SUBSTRING_INDEX(TRIM(size), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Dài: ([0-9.\\,]+)mm \\| Rộng: ([0-9.\\,]+) mm \\| Cao: ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Dài: ', -1), 'mm', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm \\(Cao x Rộng x Dày\\)$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' ', -1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+)x([0-9.\\,]+)x([0-9.\\,]+)mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x', 1), ' ', -1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Cao ([0-9.\\,]+) mm x Rộng ([0-9.\\,]+) mm x Dày ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Cao ', -1), ' mm', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) mm x ([0-9.\\,]+) mm x ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' mm x', 1), ' ', -1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)cm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' ', -1) AS DECIMAL(18,2)) * 10
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm \\(Dài x Rộng x Cao\\)$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' ', -1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)mm \\* ([0-9.,]+)mm \\* ([0-9.,]+)mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'mm *', 1), ' ', -1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)mm \\* ([0-9.,]+)mm \\* ([0-9.,]+)mm.$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'mm *', 1), ' ', -1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) ×([0-9.,]+) ×([0-9.,]+) mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' ×', 1), ' ', -1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^- Bàn phím: Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm - Chuột: Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Chuột: Dài ', -1), ' cm', 1), ',', '.') AS DECIMAL(18,2)) * 10
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(TRIM(size), ' x', 1) AS DECIMAL(18,2)) 
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(TRIM(size), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) × ([0-9.,]+) × ([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(TRIM(size), ' ×', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)cm × ([0-9.,]+)cm × ([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(TRIM(size), ' ×', -1), 'cm', '') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^([0-9.,]+)\\*([0-9.,]+)\\*([0-9.,]+)mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(TRIM(size), '*', 1), 'mm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x([0-9.,]+) x ([0-9.,]+) mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(TRIM(size), ' x', 1), 'mm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Dài: ([0-9.,]+)cm Cao : ([0-9.,]+) cm Rộng : ([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Cao :', 1), 'Dài: ', -1), 'cm', '') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm Độ dài dây: ([0-9.,]+) m$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'ngang', 1), 'Dài ', -1), ' cm', '') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^Cao ([0-9.,]+) mm x Rộng ([0-9.,]+) mm x Sâu ([0-9.,]+) mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Rộng', 1), 'Cao ', -1), ' mm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) in \\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ mm\\)$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'mm', 1), '(', -1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Cao ([0-9.,]+)cm x Rộng ([0-9.,]+)cm x Dày ([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Cao ', -1), 'cm', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^([0-9.,]+)x([0-9.,]+)x([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x', 1), 'cm', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP 'Chuột : Dài: ([0-9.,]+)cm, Cao : ([0-9.,]+) cm, Rộng : ([0-9.,]+)cm' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Chuột : Dài: ', -1), 'cm', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm \\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ in\\)' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' cm', 1) AS DECIMAL(18,2))*10
			WHEN TRIM(size) LIKE '%pin%' THEN NULL
            WHEN TRIM(size) LIKE '' THEN NULL
           ELSE NULL
       END AS length,
       CASE

			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) cm$' THEN
               CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1), ',', '.') AS DECIMAL(18,2)) * 10
			WHEN TRIM(size) REGEXP '^Cao: ([0-9.\\,]+) mm x Rộng: ([0-9.\\,]+) mm x Dày: ([0-9.\\,]+) mm$' THEN
               CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x Dày: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+)mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Dài: ([0-9.\\,]+)mm \\| Rộng: ([0-9.\\,]+) mm \\| Cao: ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Rộng: ', -1), ' mm', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm \\(Cao x Rộng x Dày\\)$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 2), ' x', -1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+)x([0-9.\\,]+)x([0-9.\\,]+)mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x', -2), 'x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Cao ([0-9.\\,]+) mm x Rộng ([0-9.\\,]+) mm x Dày ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Rộng ', -1), ' mm', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) mm x ([0-9.\\,]+) mm x ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)cm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1) AS DECIMAL(18,2)) * 10
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm \\(Dài x Rộng x Cao\\)$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 2), ' x', -1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)mm \\* ([0-9.,]+)mm \\* ([0-9.,]+)mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'mm *', -2), ' *', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)mm \\* ([0-9.,]+)mm \\* ([0-9.,]+)mm.$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'mm *', -2), ' *', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) ×([0-9.,]+) ×([0-9.,]+) mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' ×', -2), ' ×', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^- Bàn phím: Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm - Chuột: Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Chuột: Dài ', -1), ' cm - ngang ', -1), ' cm', '') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 2), ' x', -1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) × ([0-9.,]+) × ([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' ×', -2), ' ×', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)cm × ([0-9.,]+)cm × ([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' ×', 2), ' ×', -1), 'cm', '') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^([0-9.,]+)\\*([0-9.,]+)\\*([0-9.,]+)mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), '*', 2), '*', -1), 'mm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x([0-9.,]+) x ([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Dài: ([0-9.,]+)cm Cao : ([0-9.,]+) cm Rộng : ([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(size, 'Rộng : ', -1)), 'cm', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm Độ dài dây: ([0-9.,]+) m$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'cao', 1), 'ngang ', -1), ' cm', '') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^Cao ([0-9.,]+) mm x Rộng ([0-9.,]+) mm x Sâu ([0-9.,]+) mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Sâu', 1), 'Rộng ', -1), ' mm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) in \\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ mm\\)$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x', -2), ' mm', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Cao ([0-9.,]+)cm x Rộng ([0-9.,]+)cm x Dày ([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Rộng ', -1), 'cm', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^([0-9.,]+)x([0-9.,]+)x([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x', -2), 'x', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP 'Chuột : Dài: ([0-9.,]+)cm, Cao : ([0-9.,]+) cm, Rộng : ([0-9.,]+)cm' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Rộng : ', -1), 'cm', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm \\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ in\\)' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -4), ' x', 1) AS DECIMAL(18,2))*10
			WHEN TRIM(size) LIKE '%pin%' THEN NULL
            WHEN TRIM(size) LIKE '' THEN NULL
           ELSE NULL
       END AS width,
       CASE
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) cm$' THEN
               CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -1), ' cm', 1), ',', '.') AS DECIMAL(18,2)) * 10 
			WHEN TRIM(size) REGEXP '^Cao: ([0-9.\\,]+) mm x Rộng: ([0-9.\\,]+) mm x Dày: ([0-9.\\,]+) mm$' THEN
               CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x Dày: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -1), ' x', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+)mm$' THEN
               CAST(REPLACE(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(TRIM(size), ' x ', -1)), 'mm', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Dài: ([0-9.\\,]+)mm \\| Rộng: ([0-9.\\,]+) mm \\| Cao: ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Cao: ', -1), ' mm', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm \\(Cao x Rộng x Dày\\)$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 3), ' x', -1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+)x([0-9.\\,]+)x([0-9.\\,]+)mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x', -1), 'mm', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Cao ([0-9.\\,]+) mm x Rộng ([0-9.\\,]+) mm x Dày ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Dày ', -1), ' mm', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) mm x ([0-9.\\,]+) mm x ([0-9.\\,]+) mm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -1), ' mm', 1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)cm$' THEN
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -1), ' cm', 1) AS DECIMAL(18,2)) * 10
			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) mm \\(Dài x Rộng x Cao\\)$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 3), ' x', -1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)mm \\* ([0-9.,]+)mm \\* ([0-9.,]+)mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'mm *', -1), ' *', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)mm \\* ([0-9.,]+)mm \\* ([0-9.,]+)mm.$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'mm *', -1), ' *', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) ×([0-9.,]+) ×([0-9.,]+) mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX((SUBSTRING_INDEX(TRIM(size), ' mm', 1)), ' ×', -1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^- Bàn phím: Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm - Chuột: Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(TRIM(size), 'cao ', -1), ' cm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(TRIM(size), ' x', -1) AS DECIMAL(18,2)) 
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x([0-9.,]+) mm$' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -1), ' x', -1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) × ([0-9.,]+) × ([0-9.,]+) mm$' THEN
				 CAST(SUBSTRING_INDEX(TRIM(size), ' ×', -1) AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+)cm × ([0-9.,]+)cm × ([0-9.,]+)cm$' THEN
				 CAST(REPLACE(SUBSTRING_INDEX(TRIM(size), ' ×', 1), 'cm', '') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^([0-9.,]+)\\*([0-9.,]+)\\*([0-9.,]+)mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(TRIM(size), '*', -1), 'mm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x([0-9.,]+) x ([0-9.,]+) mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(TRIM(size), ' x', -1), 'mm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Dài: ([0-9.,]+)cm Cao : ([0-9.,]+) cm Rộng : ([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Rộng :', 1), 'Cao : ', -1), 'cm', '') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Dài ([0-9.,]+) cm - ngang ([0-9.,]+) cm - cao ([0-9.,]+) cm Độ dài dây: ([0-9.,]+) m$' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'cao ', -1), ' cm', 1) AS DECIMAL(18,2)) * 10
			WHEN TRIM(size) REGEXP '^Cao ([0-9.,]+) mm x Rộng ([0-9.,]+) mm x Sâu ([0-9.,]+) mm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x Sâu ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) in \\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ mm\\)$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
			WHEN TRIM(size) REGEXP '^Cao ([0-9.,]+)cm x Rộng ([0-9.,]+)cm x Dày ([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Dày ', -1), 'cm', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP '^([0-9.,]+)x([0-9.,]+)x([0-9.,]+)cm$' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'x', -1), 'cm', 1), ',', '.') AS DECIMAL(18,2))*10
			WHEN TRIM(size) REGEXP 'Chuột : Dài: ([0-9.,]+)cm, Cao : ([0-9.,]+) cm, Rộng : ([0-9.,]+)cm' THEN
				CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Cao : ', -1), 'cm', 1), ',', '.') AS DECIMAL(18,2)) 
			WHEN TRIM(size) REGEXP '([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm \\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ in\\)' THEN
				CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -3), ' x', 1) AS DECIMAL(18,2))*10
			WHEN TRIM(size) LIKE '%pin%' THEN NULL
            WHEN TRIM(size) LIKE '' THEN NULL
           ELSE NULL
       END AS height,
       CASE
			WHEN weight REGEXP '^[0-9]+(\\.[0-9]{1,3})?( g| kg)$' THEN
				CASE
					WHEN weight LIKE '% g' THEN CAST(TRIM(SUBSTRING_INDEX(weight, ' ', 1)) AS DECIMAL(18,2))
					WHEN weight LIKE '% kg' THEN CAST(TRIM(SUBSTRING_INDEX(weight, ' ', 1)) AS DECIMAL(18,2)) * 1000
					ELSE NULL
				END
			ELSE NULL  -- Các giá trị không hợp lệ hoặc không khớp định dạng
		END AS weight,
		dpi,
		sensor,
		connector,
		pin,
		os,
		brand,
		created_at,
        'cellphoneS'
	FROM staging_mouse_cellphones;

    
    -- CLEANING
    /*
    1. Kiểm tra xem có bị trùng dữ liệu hay không bằng cách so sánh nk (có thì so sánh)
    2. Xác định các trường dữ liệu quan trọng, tiến hành tiền xử lý dữ liệu tùy theo ý
    */
	DELETE s1
	FROM 
		staging_mouse_daily s1
	JOIN 
		staging_mouse_daily s2
	ON 
		s1.product_name = s2.product_name 
        and s1.manufacturer = s2.manufacturer
		AND s1.id > s2.id
	WHERE 
		s1.id > 0;

    DELETE FROM staging_mouse_daily
	WHERE 
		(product_name IS NULL                      -- Kiểm tra tên sản phẩm không được để trống
		OR price IS NULL) and id >0;  

    
    
end //
delimiter ;

/*LOAD DATE_DIM*/
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\date_dim_without_quarter.csv'
INTO TABLE date_dim
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 0 ROWS;


	
