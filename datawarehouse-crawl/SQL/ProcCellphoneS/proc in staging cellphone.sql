use dbstaging;

/*QUERY LOAD DATA INFILE
LẤY FILE CÓ TRẠNG THÁI LÀ L_RE (LOAD READY EXTRACT) ĐỂ ĐƯA VÀO CÂU LỆNH LOAD DATA IFILE
MỤC ĐÍCH CỦA PROCEDURE LÀ LẤY ĐƯỢC QUERY LOAD CSV VÀO STAGING ĐỂ EXEC Ở JAVA
*/
drop procedure load_csv_to_temp_staging_cellphone;
delimiter //
create procedure load_csv_to_temp_staging_cellphone( in date_load_data date)
begin
	/*Khai báo biến với
		file_paths : đường dẫn của file csv
        fields_terminated : kí tự phân tách giữa các trường trong file csv -> dấu phẩy
        optionally_enclosed : kí tự bao quanh giá trị trong từng trường -> Dấu "
        lines_terminated : kí tự kết thúc một dòng -> \n
        ignore_row : số dòng cần bỏ qua trong file csv
        table_staging : tên bảng sẽ load data từ csv vào staging
        stg_fields : tên các cột của bảng staging đó
        log_id : id của của file log
    */
	declare file_paths varchar(255);
    declare fields_terminated varchar(10);
	declare optionally_enclosed varchar(10);
	declare lines_terminated varchar(10);
	declare ignore_row	int;
    declare table_staging varchar(50);
    DECLARE stg_fields text;
    declare log_id int;

    /*
		Kiểm tra date_load_data - ngày lấy file để load vào staging,
        nếu biến này không được truyền thì để mặc định là ngày hiện tại
    */
    IF date_load_data IS NULL THEN
        SET date_load_data = CURDATE();
END IF;

    /*
		Lấy các thuộc tính cần thiết từ các bảng liên quan để xây dựng câu lệnh `LOAD DATA INFILE`
		nhằm tải dữ liệu từ tệp CSV vào bảng staging.
	*/
select
    fl.file_path, 				-- Đường dẫn tệp CSV cần nạp vào hệ thống (file_paths).
    cf.fields_terminated_by, 	-- Ký tự phân tách các trường (fields) trong tệp CSV (fields_terminated).
    cf.optionally_enclosed_by, 	-- Ký tự tùy chọn bao quanh các giá trị trong tệp CSV (optionally_enclosed).
    cf.lines_terminated_by, 	-- Ký tự hoặc chuỗi kết thúc dòng trong tệp CSV (lines_terminated).
    cf.ignore_rows, 			-- Số hàng đầu tiên trong tệp CSV cần bỏ qua, thường là dòng tiêu đề (ignore_row).
    cf.staging_fields, 			-- Danh sách các cột trong bảng staging tương ứng với tệp CSV (stg_fields).
    fl.file_log_id, 			-- ID bản ghi trong bảng `file_logs`, đại diện cho tệp đang được xử lý (log_id).
    cf.tble_staging			-- Tên bảng staging sẽ được sử dụng để lưu dữ liệu từ tệp CSV (table_staging).
into
    file_paths,fields_terminated, optionally_enclosed,
    lines_terminated, ignore_row, stg_fields, log_id, table_staging
from
    dbcontrol.file_logs fl -- Bảng ghi lại thông tin các tệp đã tải lên hệ thống.
        join
    dbcontrol.configs cf -- Bảng chứa thông tin cấu hình cho các tệp CSV, bao gồm định dạng và bảng đích.
    on fl.config_id = cf.config_id
where
    fl.status = 'C_SE' -- Chỉ lấy các bản ghi trong bảng `file_logs` có trạng thái "C_SE"
  AND DATE(fl.update_at) = date_load_data -- Lọc các bản ghi được cập nhật trong ngày được chỉ định (`date_load_data`).
  and fl.config_id = 2
    limit 1;

-- Kiểm tra xem biến `file_paths` (chứa đường dẫn tệp) có giá trị NULL hay không.
if file_paths is null then
		-- Nếu giá trị của `file_paths` là NULL, điều này có nghĩa:
		-- 1. Không có tệp nào được tìm thấy với trạng thái 'C_SE' (trạng thái sẵn sàng để tải).
		-- 2. Truy vấn trước đó không trả về kết quả
		-- Sử dụng câu lệnh SIGNAL để phát sinh một lỗi tùy chỉnh (user-defined error).
		signal sqlstate '45000'  -- Mã lỗi SQLSTATE tùy chỉnh '45000'
		set message_text = 'Error: File path is NULL or no file with C_SE status!';
		-- Thông báo lỗi được hiển thị sẽ là:
		-- "Error: File path is NULL or no file with C_SE status!"
		-- Mục đích là cảnh báo người dùng hoặc hệ thống rằng không có tệp nào để xử lý.
end if;

    -- Tạo load data infile động
    SET @sql = CONCAT(
        "LOAD DATA INFILE '", file_paths, "' ", -- Tạo câu lệnh để chỉ định tệp cần nạp dữ liệu, sử dụng giá trị trong biến `file_paths`.
        "INTO TABLE dbstaging.", table_staging, " ", -- Chỉ định bảng mục tiêu trong database `dbstaging`, tên bảng được lấy từ biến `table_staging`.
        "FIELDS TERMINATED BY '", fields_terminated, "' ", -- Định nghĩa ký tự phân cách giữa các trường trong file CSV. Giá trị này được lấy từ cấu hình (`fields_terminated`).
        "OPTIONALLY ENCLOSED BY '", optionally_enclosed, "' ", -- Định nghĩa ký tự bao quanh dữ liệu. Giá trị được lấy từ cấu hình (`optionally_enclosed`).
        "LINES TERMINATED BY '", lines_terminated, "' ", -- Định nghĩa ký tự kết thúc mỗi dòng trong file dữ liệu. Giá trị được lấy từ cấu hình (`lines_terminated`).
        "IGNORE ", ignore_row, " ROWS ", -- Số dòng cần bỏ qua được lấy từ cấu hình (`ignore_row`).
        "(", stg_fields, ");" -- Sử dụng các trường trong staging_fields
    );
    -- Xuất câu truy vấn SQL đã tạo ra
SELECT @sql AS debug_query, log_id AS file_log_id;

end //
delimiter ;

/*TRANSFORM + CLEANING DỮ LIỆU*/
drop procedure if exists transform_and_cleaning_data;
delimiter //
create procedure transform_and_cleaning_data()
begin
	/*
		Thiết lập lại kích thước tối đa của kết quả trả về bởi hàm GROUP_CONCAT() trong session được tính bằng byte
        Được dùng để xử lý việc transfrom trong bảng staging
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
        /*
			Transform cho giá trị price -  giá của chuột
            Giá trị trước khi transform
				269.100đ
            Giá trị sau khi transform
				269.100
        */
		CASE
			WHEN price = 'Giá Liên Hệ' THEN NULL  -- Nếu giá là "Giá Liên Hệ", đặt thành NULL
			WHEN price IS NULL OR price = '' THEN NULL  -- Trường hợp giá rỗng hoặc NULL
			WHEN price REGEXP '^[0-9]+(\\.[0-9]{1,3})?đ$' THEN  -- Kiểm tra định dạng số kèm "đ"
				CAST(REPLACE(REPLACE(price, 'đ', ''), '.', '') AS DECIMAL(18,2))  -- Chuyển đổi thành DECIMAL
			ELSE
				NULL  -- Các trường hợp không xác định, đặt thành NULL
		END AS price,

        /*
			Transform cột image
            Giá trị cột trước khi transform:
				https://cdn2.cellphones.com.vn/insecure/rs:fill:358:358/q:90/plain/https://cellphones.com.vn/media/catalog/product/g/r/group_78_1_.png
			Giá trị cột sau khi transform:
				https://cellphones.com.vn/media/catalog/product/g/r/group_78_1_.png
        */
		(
        -- Kết hợp các giá trị hình ảnh thành một chuỗi duy nhất, phân cách bằng dấu phẩy và khoảng trắng
        SELECT GROUP_CONCAT(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(image, 'plain/', -1), ',', 1)) SEPARATOR ', ')
			FROM (
				-- Trích xuất các giá trị hình ảnh từ cột images
				SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(images, 'plain/', -1), ',', n.n)) AS image
				FROM staging_mouse_cellphones
				JOIN (
					-- Tạo một bảng phụ chứa các số từ 1 đến 10 để tách chuỗi hình ảnh
					SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
					UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
					UNION ALL SELECT 9 UNION ALL SELECT 10
				) n ON CHAR_LENGTH(images) - CHAR_LENGTH(REPLACE(images, ',', '')) >= n.n - 1 -- Đảm bảo tách đúng số phần tử trong chuỗi images
				WHERE images IS NOT NULL -- Chỉ lấy những dòng có giá trị trong cột images
			) AS temp -- Đặt tên cho bảng tạm thời chứa các giá trị hình ảnh đã tách
		) AS image, -- Đặt tên cho cột kết quả cuối cùng là image

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

			WHEN TRIM(size) REGEXP '^([0-9.\\,]+) x ([0-9.\\,]+) x ([0-9.\\,]+) cm$' THEN
               CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), ' x', 1), ' ', -1), ',', '.') AS DECIMAL(18,2)) * 10

			WHEN TRIM(size) REGEXP '^Cao: ([0-9.\\,]+) mm x Rộng: ([0-9.\\,]+) mm x Dày: ([0-9.\\,]+) mm$' THEN
              CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(size), 'Cao: ', -1), ' mm', 1), ',', '.') AS DECIMAL(18,2))

            -- Trường hợp kích thước có 3 giá trị x x x mm ở cuối chuỗi
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
			-- Kiểm tra xem giá trị weight có phù hợp với định dạng hợp lệ (số theo sau là 'g' hoặc 'kg') không
			WHEN weight REGEXP '^[0-9]+(\\.[0-9]{1,3})?( g| kg)$' THEN
				-- CASE lồng để xử lý đơn vị ('g' hoặc 'kg')
				CASE
					-- Nếu giá trị weight có chứa 'g', chuyển đổi nó thành kiểu DECIMAL
					WHEN weight LIKE '% g' THEN CAST(TRIM(SUBSTRING_INDEX(weight, ' ', 1)) AS DECIMAL(18,2))

                    -- Nếu giá trị weight có chứa 'kg', nhân với 1000 để chuyển đổi thành gam và chuyển sang kiểu DECIMAL
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

    -- Xóa các bản ghi trùng lặp trong bảng staging_mouse_daily
	DELETE s1
	FROM
		staging_mouse_daily s1
	JOIN
		staging_mouse_daily s2
	ON
		s1.product_name = s2.product_name 		-- So sánh tên sản phẩm
        and s1.manufacturer = s2.manufacturer	-- So sánh nhà sản xuất
		AND s1.id > s2.id 						-- Chỉ giữ bản ghi có id nhỏ hơn, xóa bản ghi trùng lặp với id lớn hơn
	WHERE
		s1.id > 0;	-- Điều kiện chỉ xóa khi id lớn hơn 0 (bỏ qua bản ghi id = 0)

	-- Xóa các bản ghi có dữ liệu quan trọng bị thiếu (product_name hoặc price bị NULL)
    DELETE FROM staging_mouse_daily
	WHERE
		(product_name IS NULL                     	-- Kiểm tra tên sản phẩm không được để trống
		OR price IS NULL) 							-- Kiểm tra giá sản phẩm không được để trống
        and id >0;

end //
delimiter ;

/*LOAD DATE_DIM*/
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\date_dim_without_quarter.csv'
INTO TABLE date_dim
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 0 ROWS;



