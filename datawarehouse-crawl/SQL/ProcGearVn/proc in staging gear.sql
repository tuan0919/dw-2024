use dbstaging;

/*QUERY LOAD DATA INFILE
LẤY FILE CÓ TRẠNG THÁI LÀ L_RE (LOAD READY EXTRACT) ĐỂ ĐƯA VÀO CÂU LỆNH LOAD DATA IFILE
MỤC ĐÍCH CỦA PROCEDURE LÀ LẤY ĐƯỢC QUERY LOAD CSV VÀO STAGING ĐỂ EXEC Ở JAVA
*/
call load_csv_to_temp_staging_gearvn(null);
drop procedure load_csv_to_temp_staging_gearvn;
delimiter //
create procedure load_csv_to_temp_staging_gearvn( in date_load_data date)
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
    where fl.status = 'C_SE' AND DATE(fl.update_at) = date_load_data and fl.config_id =3
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








/*TRANSFORM + CLEANING DỮ LIỆU*/
drop procedure if exists transform_and_cleaning_data_gearvn;
delimiter //
create procedure transform_and_cleaning_data_gearvn()
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
    DROP TEMPORARY TABLE IF EXISTS staging_combined_gearvn;
	DROP TEMPORARY TABLE IF EXISTS temp_product;
    
    CREATE TEMPORARY TABLE staging_combined_gearvn (
        id INT AUTO_INCREMENT PRIMARY KEY,
		product_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        price DECIMAL(18,2),
		image MEDIUMTEXT,
		length decimal(18,2),
        width decimal(18,2),
        height decimal(18,2),
		weight decimal(18,2),
		resolution VARCHAR(255),
		sensor VARCHAR(255),
		connectivity VARCHAR(255),
		battery VARCHAR(255),
		compatibility VARCHAR(255),
		manufacturer VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        source varchar(255)
    );
    
	SET SESSION group_concat_max_len = 1000000;  -- Hoặc giá trị lớn hơn tùy theo nhu cầu

    -- insert bảng staging_cellphones vào bảng tạm
	INSERT INTO staging_combined_gearvn (
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
			WHEN price IS NULL OR price = '' THEN NULL  -- Handle NULL or empty strings
			WHEN price REGEXP '^[0-9]{1,3}(\\.[0-9]{3})*₫$' THEN  -- Check for format with "₫" and "." separators
				CAST(REPLACE(REPLACE(price, '₫', ''), '.', '') AS DECIMAL(18,2))  -- Remove "₫" and "." then cast to DECIMAL
			ELSE 
				NULL  -- Set to NULL for unidentified cases
		END AS price,
		images,
       CASE
           WHEN size REGEXP 'Dài\\s([0-9]+(\\.[0-9]+)?)\\s*x\\s*Rộng\\s([0-9]+(\\.[0-9]+)?)\\s*x\\s*Cao\\s([0-9]+(\\.[0-9]+)?)' THEN
            CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x Rộng', 1), 'Dài ', -1) AS DECIMAL(18,2))  -- Extract length
        WHEN size REGEXP '^[0-9]+(\\.[0-9]+)?\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*(mm|cm)?$' THEN
            CAST(
                CASE
                    WHEN size LIKE '%cm%' THEN
                        SUBSTRING_INDEX(REPLACE(size, 'x', ' x'), ' x', 1) * 10  -- Convert cm to mm
                    ELSE
                        SUBSTRING_INDEX(REPLACE(size, 'x', ' x'), ' x', 1)  -- Use mm value directly
                END AS DECIMAL(18,2)
            )
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)?\\s*mm\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*mm\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*mm$' THEN
            CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', 1), ' mm', 1), ',', '.') AS DECIMAL(18,2)) -- Handle mm
		WHEN size REGEXP '([0-9]+(\\.[0-9]+)?)\\*([0-9]+(\\.[0-9]+)?)\\*([0-9]+(\\.[0-9]+)?)mm' THEN
            CAST(SUBSTRING_INDEX(size, '*', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' ', -1), ',', '.') AS DECIMAL(18,2)) * 10
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' ', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)mm, ([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' ', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^Chiều cao: ([0-9.,]+) mm, Chiều rộng: ([0-9.,]+) mm, Chiều dày: ([0-9.,]+) mm(, Trọng lượng: ([0-9.,]+) g)?$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Chiều cao: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+ x [0-9.,]+ x [0-9.,]+ \\( mm \\) \\( Dài x Rộng x Cao \\)$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x ', 1), ' ', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+\\(L\\)x[0-9.,]+\\(w\\)x[0-9.,]+\\(H\\) mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, '(L)', 1), 'x', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+\\(L\\) x [0-9.,]+\\(W\\) x [0-9.,]+\\(H\\) mm$'THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, '(L)', 1), 'x', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(REPLACE(size, UNHEX('E2808E'), '')) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(REPLACE(size, UNHEX('E2808E'), '')), ' x', 1), ' ', -1), ',', '.') AS DECIMAL(18,2)) * 10
		WHEN TRIM(REPLACE(size, UNHEX('E2808E'), '')) REGEXP '^\\([0-9.,]+\\) x [0-9.,]+ x [0-9.,]+ \\( mm \\) \\( Dài x Rộng x Cao \\)$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(REPLACE(size, UNHEX('E2808E'), '')), ' x', 1), '(', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^\\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ \\( mm \\) \\( Dài x Rộng x Cao \\)$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', 1), '(', -1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? \\(mm\\)$' THEN
			CAST(SUBSTRING_INDEX(size, ' x', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? mm(, [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? mm)*$' THEN
			CAST(SUBSTRING_INDEX(size, ' x', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\,[0-9]+)? mm x [0-9]+(\\,[0-9]+)? mm x [0-9]+(\\,[0-9]+)? mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(size, ' x', 1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? \\(dài\\) x [0-9]+(\\.[0-9]+)? \\(rộng\\) x [0-9]+(\\.[0-9]+)? mm \\(cao\\)$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' (dài)', 1), ' x', -1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Length: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Width: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Height: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' mm', 1), 'Length: ', -1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Dài: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Rộng: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Cao: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Dài: ', -1), ' mm', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Chiều dài: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch Chiều rộng: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch Chiều cao: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Chiều dài: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
        ELSE NULL
       END AS length,
       CASE
		 WHEN size REGEXP 'Dài\\s([0-9]+(\\.[0-9]+)?)\\s*x\\s*Rộng\\s([0-9]+(\\.[0-9]+)?)\\s*x\\s*Cao\\s([0-9]+(\\.[0-9]+)?)' THEN
            CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x Cao', 1), 'Rộng ', -1) AS DECIMAL(18,2))  -- Extract width
        WHEN size REGEXP '^[0-9]+(\\.[0-9]+)?\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*(mm|cm)?$' THEN
            CAST(
                CASE
                    WHEN size LIKE '%cm%' THEN
                        SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(size, 'x', ' x'), ' x', 2), ' x', -1) * 10  -- Convert cm to mm
                    ELSE
                        SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(size, 'x', ' x'), ' x', 2), ' x', -1)  -- Use mm value directly
                END AS DECIMAL(18,2)
            )
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)?\\s*mm\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*mm\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*mm$' THEN
            CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', 2), ' x', -1), ' mm', '') AS DECIMAL(18,2))  -- Handle mm
		WHEN size REGEXP '([0-9]+(\\.[0-9]+)?)\\*([0-9]+(\\.[0-9]+)?)\\*([0-9]+(\\.[0-9]+)?)mm' THEN
            CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, '*', 2), '*', -1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1), ',', '.') AS DECIMAL(18,2)) * 10
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)mm, ([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^Chiều cao: ([0-9.,]+) mm, Chiều rộng: ([0-9.,]+) mm, Chiều dày: ([0-9.,]+) mm(, Trọng lượng: ([0-9.,]+) g)?$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Chiều rộng: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+ x [0-9.,]+ x [0-9.,]+ \\( mm \\) \\( Dài x Rộng x Cao \\)$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x ', 2), ' x ', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+\\(L\\)x[0-9.,]+\\(w\\)x[0-9.,]+\\(H\\) mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, '(w)', 1), 'x', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+\\(L\\) x [0-9.,]+\\(W\\) x [0-9.,]+\\(H\\) mm$'THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, '(W)', 1), 'x', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(REPLACE(size, UNHEX('E2808E'), '')) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -2), ' x', 1), ',', '.') AS DECIMAL(18,2)) * 10
		WHEN TRIM(size) REGEXP '^\\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ \\( mm \\) \\( Dài x Rộng x Cao \\)$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', -4), ' x', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? \\(mm\\)$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', -2), ' x', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? mm(, [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? mm)*$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', -2), ' x', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\,[0-9]+)? mm x [0-9]+(\\,[0-9]+)? mm x [0-9]+(\\,[0-9]+)? mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', 2), ' x', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? \\(dài\\) x [0-9]+(\\.[0-9]+)? \\(rộng\\) x [0-9]+(\\.[0-9]+)? mm \\(cao\\)$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' (rộng)', 1), ' x', -1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Length: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Width: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Height: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' mm', 2), 'Width: ', -1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Dài: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Rộng: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Cao: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Rộng: ', -1), ' mm', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Chiều dài: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch Chiều rộng: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch Chiều cao: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Chiều rộng: ', -1), ' mm', 1) AS DECIMAL(18,2))
        ELSE NULL
       END AS width,
       CASE
		WHEN size REGEXP 'Dài\\s([0-9]+(\\.[0-9]+)?)\\s*x\\s*Rộng\\s([0-9]+(\\.[0-9]+)?)\\s*x\\s*Cao\\s([0-9]+(\\.[0-9]+)?)' THEN
            CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x Cao', -1), 'Cao ', -1) AS DECIMAL(18,2))  -- Extract height
        WHEN trim(size) REGEXP '^[0-9]+(\\.[0-9]+)?\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*(mm)?$' THEN
            CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(size, 'x', ' x'), ' x', 3), ' x', -1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)?\\s*mm\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*mm\\s*x\\s*[0-9]+(\\.[0-9]+)?\\s*mm$' THEN
            CAST(REPLACE(SUBSTRING_INDEX(size, ' x', -1), ' mm', '') AS DECIMAL(18,2))  -- Handle mm
		WHEN size REGEXP '([0-9]+(\\.[0-9]+)?)\\*([0-9]+(\\.[0-9]+)?)\\*([0-9]+(\\.[0-9]+)?)mm' THEN
            CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, '*', -1), 'mm', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -1), ' cm', 1), ',', '.') AS DECIMAL(18,2)) * 10
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)mm, ([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+)mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', -1), 'mm', 1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^Chiều cao: ([0-9.,]+) mm, Chiều rộng: ([0-9.,]+) mm, Chiều dày: ([0-9.,]+) mm(, Trọng lượng: ([0-9.,]+) g)?$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Chiều dày: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+ x [0-9.,]+ x [0-9.,]+ \\( mm \\) \\( Dài x Rộng x Cao \\)$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX('116.6 x 62.15 x 38.2 ( mm ) ( Dài x Rộng x Cao )', ' x', -3), ' ', 2), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+\\(L\\)x[0-9.,]+\\(w\\)x[0-9.,]+\\(H\\) mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, '(H)', 1), 'x', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9.,]+\\(L\\) x [0-9.,]+\\(W\\) x [0-9.,]+\\(H\\) mm$'THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, '(H)', 1), 'x', -1), ',', '.') AS DECIMAL(18,2))
		WHEN TRIM(REPLACE(size, UNHEX('E2808E'), '')) REGEXP '^([0-9.,]+) x ([0-9.,]+) x ([0-9.,]+) cm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(REPLACE(size, UNHEX('E2808E'), '')), ' x', -1), ' cm', 1), ',', '.') AS DECIMAL(18,2)) * 10
		WHEN TRIM(size) REGEXP '^\\([0-9.,]+ x [0-9.,]+ x [0-9.,]+ \\( mm \\) \\( Dài x Rộng x Cao \\)$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', -3), ' ( mm )', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? \\(mm\\)$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', -1), ' (mm)', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? mm(, [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? x [0-9]+(\\.[0-9]+)? mm)*$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' x', -1), ' mm', 1) AS DECIMAL(18,2)) 
		WHEN TRIM(size) REGEXP '^[0-9]+(\\,[0-9]+)? mm x [0-9]+(\\,[0-9]+)? mm x [0-9]+(\\,[0-9]+)? mm$' THEN
			CAST(REPLACE(SUBSTRING_INDEX(size, ' x', -1), ' mm', '') AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP '^[0-9]+(\\.[0-9]+)? \\(dài\\) x [0-9]+(\\.[0-9]+)? \\(rộng\\) x [0-9]+(\\.[0-9]+)? mm \\(cao\\)$' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, ' mm', 1), ' x', -1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Length: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Width: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Height: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Height: ', -1), ' mm', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Dài: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Rộng: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in, Cao: [0-9]+(\\.[0-9]+)? mm / [0-9]+(\\.[0-9]+)? in' THEN
			CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Cao: ', -1), ' mm', 1) AS DECIMAL(18,2))
		WHEN TRIM(size) REGEXP 'Chiều dài: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch Chiều rộng: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch Chiều cao: [0-9]+(\\,[0-9]+)? mm / [0-9]+(\\,[0-9]+)? inch' THEN
			CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(size, 'Chiều cao: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))
        ELSE NULL
       END AS height,
       CASE
        WHEN weight REGEXP '^[<]?[0-9]+(\\.[0-9]+)?( g| kg| gram| grams| gr|g|gr)(\\s*\\(.*\\))?$' THEN
            -- Handle cases like '60 g', '60 g (extra text)', and '< 60 g'
            CAST(REPLACE(REPLACE(TRIM(SUBSTRING_INDEX(weight, ' ', 1)), '<', ''), 'g', '') AS DECIMAL(18,2))
        WHEN weight REGEXP '^[0-9]+(\\.[0-9]+)? g / [0-9]+(\\,[0-9]+)? oz.*$' THEN
            -- Handle cases like '60 g / 3,56 oz'
            CAST(TRIM(SUBSTRING_INDEX(weight, ' g', 1)) AS DECIMAL(18,2))
        WHEN weight REGEXP '^<\\s*[0-9]+(\\.[0-9]+)?\\s*g$' THEN
            -- Handle specific case like '< 60 g'
            CAST(REPLACE(REPLACE(TRIM(SUBSTRING(weight, 2)), 'g', ''), ' ', '') AS DECIMAL(18,2))
        WHEN weight REGEXP '^[0-9]+(\\,[0-9]+)?\\s*g.*$' THEN
            -- Handle specific case like '101,4 g - trọng lượng bao gồm pin (25g) và bộ thu (1,8g)'
            CAST(REPLACE(TRIM(SUBSTRING_INDEX(weight, ' g', 1)), ',', '.') AS DECIMAL(18,2)) -- Replace comma with dot
		WHEN weight REGEXP '^[a-zA-Z0-9]+\\s+[0-9]+(\\,[0-9]+)?\\s*g.*$' THEN
            -- Handle case like 'POWERPLAY 68g / Pin AA 86g'
            CAST(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(weight, 'g', 1), ' ', -1)) AS DECIMAL(18,2))
        ELSE NULL
		END AS weight,
		dpi,
		sensor,
		connector,
		pin,
		os,
		brand,
		created_at,
        'gearVn'
	FROM staging_gearvn;

    
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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_name, manufacturer, weight, resolution ORDER BY manufacturer) AS row_num
        FROM dbstaging.staging_combined_gearvn
    ) AS temp
    WHERE row_num = 1;
    SET SQL_SAFE_UPDATES = 0;

    DELETE FROM temp_product
	WHERE 
		product_name IS NULL                      -- Kiểm tra tên sản phẩm không được để trống
		OR price IS NULL;  
	SET SQL_SAFE_UPDATES = 1;

    -- chuyển mã cho chắc :v
    ALTER TABLE temp_product
	CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    
    -- Tiến hành insert dữ liệu đã được xử lý vào staging
    INSERT INTO staging_mouse_daily_gearvn  (
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
    FROM 
        temp_product;
    
end //
delimiter ;

