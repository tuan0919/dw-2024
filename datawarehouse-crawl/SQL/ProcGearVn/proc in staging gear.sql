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
    declare ignore_row int;
    declare table_staging varchar(50);
    declare stg_fields text;
    declare log_id int;

    IF date_load_data IS NULL THEN
        SET date_load_data = CURDATE();
END IF;

select
    fl.file_path,
    cf.fields_terminated_by,
    cf.optionally_enclosed_by,
    cf.lines_terminated_by,
    cf.ignore_rows,
    cf.staging_fields,
    fl.file_log_id,
    cf.tble_staging
into
    file_paths, fields_terminated, optionally_enclosed,
    lines_terminated, ignore_row, stg_fields, log_id, table_staging
from
    dbcontrol.file_logs fl
        join
    dbcontrol.configs cf
    on fl.config_id = cf.config_id
where
    fl.status = 'C_SE'
  AND DATE(fl.create_time) = date_load_data
  and fl.config_id = 3
    limit 1;

if file_paths is null then
        signal sqlstate '45000'
        set message_text = 'Error: File path is NULL or no file with C_SE status!';
end if;

    -- Tách các câu lệnh SQL thành hai biến riêng biệt
    SET @truncate_sql = CONCAT("TRUNCATE TABLE dbstaging.staging_gearvn;");
    SET @load_sql = CONCAT(
        "LOAD DATA INFILE '", file_paths, "' ", -- Tạo câu lệnh để chỉ định tệp cần nạp dữ liệu, sử dụng giá trị trong biến `file_paths`.
        "INTO TABLE dbstaging.", table_staging, " ", -- Chỉ định bảng mục tiêu trong database `dbstaging`, tên bảng được lấy từ biến `table_staging`.
        "FIELDS TERMINATED BY '", fields_terminated, "' ", -- Định nghĩa ký tự phân cách giữa các trường trong file CSV. Giá trị này được lấy từ cấu hình (`fields_terminated`).
        "OPTIONALLY ENCLOSED BY '", optionally_enclosed, "' ", -- Định nghĩa ký tự bao quanh dữ liệu. Giá trị được lấy từ cấu hình (`optionally_enclosed`).
        "LINES TERMINATED BY '", lines_terminated, "' ", -- Định nghĩa ký tự kết thúc mỗi dòng trong file dữ liệu. Giá trị được lấy từ cấu hình (`lines_terminated`).
        "IGNORE ", ignore_row, " ROWS ", -- Số dòng cần bỏ qua được lấy từ cấu hình (`ignore_row`).
        "(", stg_fields, ");" -- Sử dụng các trường trong staging_fields
    );
    SET @log_id = log_id;

    -- Xuất các câu lệnh SQL đã tạo
SELECT @truncate_sql AS truncate_query;
SELECT @load_sql AS load_query;
SELECT @log_id AS file_log_id;

end //
delimiter ;


/*TRANSFORM + CLEANING DỮ LIỆU*/
drop procedure if exists transform_and_cleaning_data_gearvn;
delimiter //
create procedure transform_and_cleaning_data_gearvn()
begin
	-- TRANSFROM
    /*
		Thiết lập lại kích thước tối đa của kết quả trả về bởi hàm GROUP_CONCAT() trong session được tính bằng byte
        Được dùng để xử lý việc transfrom trong bảng staging
    */

	SET SESSION group_concat_max_len = 1000000;  -- Hoặc giá trị lớn hơn tùy theo nhu cầu

    -- insert bảng staging_gearvn vào bảng tạm
	INSERT INTO staging_mouse_daily_gearvn (
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
        /*
			Transform cho giá trị price -  giá của chuột
            Giá trị trước khi transform
				269.100đ
            Giá trị sau khi transform
				269.100
        */
		CASE
			WHEN price IS NULL OR price = '' THEN NULL  -- Handle NULL or empty strings
			WHEN price REGEXP '^[0-9]{1,3}(\\.[0-9]{3})*₫$' THEN  -- Check for format with "₫" and "." separators
				CAST(REPLACE(REPLACE(price, '₫', ''), '.', '') AS DECIMAL(18,2))  -- Remove "₫" and "." then cast to DECIMAL
			ELSE
				NULL  -- Set to NULL for unidentified cases
		END AS price,
		images,
        /*
			Transform để lấy giá trị từ size để chuyển vào trong length (chiểu dài của chuột)
            Quá trình này diễn ra tương tự trong lúc lấy giá trị từ size sang height (cao) và width (rộng)
            Xử lý bằng cách sử dụng regexp cho từng chuỗi được loại bỏ khoảng trống ở đầu và cuối (sử dụng hàm trim())
            Quá trình này thực hiện việc chuẩn hóa và chuyển đổi kích thước sản phẩm từ nhiều định dạng khác nhau sang một định dạng thống nhất.
            Các điều kiện kiểm tra kích thước được sử dụng để nhận dạng các kiểu dữ liệu khác nhau trong trường "size" và chuyển đổi các giá trị sang dạng chuẩn (mm).
            Cụ thể:
				Các kích thước có định dạng với đơn vị cm hoặc mm sẽ được chuyển đổi sao cho tất cả đều tính bằng mm.
				Một số kích thước có thể bao gồm dấu phân cách như "x", "*", "×", hoặc có định dạng đặc biệt như "Cao", "Rộng", "Dày".
				Mỗi khi phát hiện một mẫu kích thước khớp với định dạng cụ thể, giá trị sẽ được tách ra và chuyển đổi thành kiểu số (DECIMAL).
				Mỗi điều kiện kiểm tra tương ứng với một loại định dạng kích thước khác nhau, đảm bảo tất cả các giá trị đều được xử lý đúng cách.
				Kết quả cuối cùng là các giá trị kích thước chuẩn hóa, đảm bảo tính đồng nhất cho các phép tính hoặc phân tích sau này.
        */
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
       /*
		Transform cho khối lượng chuột
        giá trị trước khi transform
			58 g
        giá trị sau khi transform
			58
		Mục đích là đưa giá trị chuột về đúng định dạng bằng cách lọc các kí tự trong chuỗi, chỉ lấy giá trị số
        đồng thời chuyển giá trị về cùng đơn vị là g (gam)
        Sử dụng regexp để phân tích và tách chuỗi
       */
       CASE
		-- Kiểm tra trường hợp như '60 g', '60 g (văn bản bổ sung)', và '< 60 g'
        WHEN weight REGEXP '^[<]?[0-9]+(\\.[0-9]+)?( g| kg| gram| grams| gr|g|gr)(\\s*\\(.*\\))?$' THEN
             -- Trích xuất số lượng trước đơn vị (g), loại bỏ dấu '<' và 'g', sau đó chuyển thành kiểu DECIMAL
            CAST(REPLACE(REPLACE(TRIM(SUBSTRING_INDEX(weight, ' ', 1)), '<', ''), 'g', '') AS DECIMAL(18,2))

        -- Kiểm tra trường hợp như '60 g / 3,56 oz'
        WHEN weight REGEXP '^[0-9]+(\\.[0-9]+)? g / [0-9]+(\\,[0-9]+)? oz.*$' THEN
            -- Trích xuất số lượng trong đơn vị gram trước dấu 'g', bỏ phần oz
            CAST(TRIM(SUBSTRING_INDEX(weight, ' g', 1)) AS DECIMAL(18,2))

        -- Kiểm tra trường hợp cụ thể như '< 60 g'
        WHEN weight REGEXP '^<\\s*[0-9]+(\\.[0-9]+)?\\s*g$' THEN
            -- Loại bỏ dấu '<' và chuyển đổi giá trị thành kiểu DECIMAL
            CAST(REPLACE(REPLACE(TRIM(SUBSTRING(weight, 2)), 'g', ''), ' ', '') AS DECIMAL(18,2))

        -- Kiểm tra trường hợp như '101,4 g - trọng lượng bao gồm pin (25g) và bộ thu (1,8g)'
        WHEN weight REGEXP '^[0-9]+(\\,[0-9]+)?\\s*g.*$' THEN
            -- Trích xuất số lượng trong đơn vị gram và thay thế dấu phẩy bằng dấu chấm để chuyển thành kiểu DECIMAL
            CAST(REPLACE(TRIM(SUBSTRING_INDEX(weight, ' g', 1)), ',', '.') AS DECIMAL(18,2)) -- Replace comma with dot

        -- Kiểm tra trường hợp như 'POWERPLAY 68g / Pin AA 86g'
        WHEN weight REGEXP '^[a-zA-Z0-9]+\\s+[0-9]+(\\,[0-9]+)?\\s*g.*$' THEN
            -- Trích xuất số lượng (68g hoặc 86g) và chuyển đổi thành kiểu DECIMAL
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
    -- Xóa các bản ghi trùng lặp trong bảng staging_mouse_daily_gearvn
	DELETE s1
	FROM
		staging_mouse_daily_gearvn s1
	JOIN
		staging_mouse_daily_gearvn s2
	ON
		s1.product_name = s2.product_name 		-- So sánh tên sản phẩm
        and s1.manufacturer = s2.manufacturer 	-- So sánh nhà sản xuất
		AND s1.id > s2.id						-- Chỉ giữ bản ghi có id nhỏ hơn, xóa bản ghi trùng lặp với id lớn hơn
	WHERE
		s1.id > 0;

    DELETE FROM staging_mouse_daily_gearvn
	WHERE
		(product_name IS NULL                      	-- Kiểm tra tên sản phẩm không được để trống
		OR price IS NULL)							-- Xóa các bản ghi có dữ liệu quan trọng bị thiếu (product_name hoặc price bị NULL)
        and id >0;


end //
delimiter ;

