use datawarehouse;
/*
Viết proc xử lý quá trình load từ staging sang dw
	1. Kiểm tra xem dữ liệu khi insert vào dw có hoàn toàn mới hay không. Nếu có -> insert
    2. Nếu dữ liệu được cập nhật -> B1: Thay đổi dòng dữ liệu trong dw với: isDelete = True, expried_date = NOW(), date_delete = NOW()
									B2: Insert dòng dữ liệu có thay đổi vào dw
*/
drop procedure if exists load_from_staging_to_dw_gearvn;
call  load_from_staging_to_dw_gearvn;
delimiter //
create procedure load_from_staging_to_dw_gearvn()
begin
	DROP temporary TABLE IF  EXISTS temp_update_products;
    DROP temporary TABLE IF  EXISTS temp_ids;
	/*
		Insert các dòng mới
        với điều kiện là chỉ thêm những sản phẩm không trùng tên và nhà sản xuất giữa staging và datawarehouse
		vào datawarehouse.product_dim
    */
	INSERT INTO datawarehouse.product_dim (
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
        source
	FROM dbstaging.staging_mouse_daily_gearvn sm
	WHERE NOT EXISTS (
		SELECT 1
		FROM datawarehouse.product_dim pd
		WHERE pd.product_name = sm.product_name
		AND pd.manufacturer = sm.manufacturer
	);


    /*Update các dòng có thay đổi*/
    /*
		Tạo bảng tạm `temp_update_products` để chứa các bản ghi trong bảng `staging_mouse_daily`
		có sự thay đổi so với bảng `product_dim` trong kho dữ liệu.
	*/
    CREATE TEMPORARY TABLE temp_update_products AS
    SELECT sm.*
    FROM dbstaging.staging_mouse_daily_gearvn sm
    WHERE EXISTS (
        SELECT 1
        FROM datawarehouse.product_dim pd2
        WHERE pd2.product_name = sm.product_name	-- Kiểm tra xem sản phẩm có cùng tên với sản phẩm trong `product_dim`
        AND pd2.isDelete = FALSE					-- Chỉ xét các sản phẩm chưa bị xóa
        AND pd2.expired_date = '9999-12-31'			-- Chỉ xét các sản phẩm chưa hết hạn
        and pd2.manufacturer = sm.manufacturer		-- Kiểm tra nhà sản xuất có khớp không
        -- Kiểm tra các trường khác nhau giữa bảng tạm và bảng trong kho dữ liệu, chỉ khi có sự thay đổi mới đưa vào bảng tạm
        AND (pd2.price <> sm.price OR					-- Kiểm tra giá có khác nhau không
             pd2.image <> sm.image OR					-- Kiểm tra hình ảnh có khác nhau không
             pd2.length <>sm.length or					-- Kiểm tra chiều dài có khác nhau không
			 pd2.width <> sm.width or					-- Kiểm tra chiều rộng có khác nhau không
			 pd2.height <> sm.height or					-- Kiểm tra chiều cao có khác nhau không
             pd2.weight <> sm.weight OR					-- Kiểm tra trọng lượng có khác nhau không
             pd2.resolution <> sm.resolution OR			-- Kiểm tra độ phân giải có khác nhau không
             pd2.sensor <> sm.sensor OR					-- Kiểm tra cảm biến có khác nhau không
             pd2.connectivity <> sm.connectivity OR		-- Kiểm tra kết nối có khác nhau không
             pd2.battery <> sm.battery OR				-- Kiểm tra pin có khác nhau không
             pd2.compatibility <> sm.compatibility OR	-- Kiểm tra tính tương thích có khác nhau không
             pd2.manufacturer <> sm.manufacturer)		-- Kiểm tra nhà sản xuất có khác nhau không
    );

    /*Tạo bảng tạm để lưu id các sản phẩm cần update thông tin được lấy từ bảng temp_update_products*/
    CREATE TEMPORARY TABLE temp_ids AS
	SELECT pd2.id
	FROM datawarehouse.product_dim pd2
	JOIN temp_update_products tup ON tup.product_name = pd2.product_name
		AND tup.manufacturer = pd2.manufacturer
	WHERE
		pd2.isDelete = FALSE 					-- Chỉ lấy các sản phẩm chưa bị xóa
		AND pd2.expired_date = '9999-12-31'; 	-- Chỉ lấy các sản phẩm chưa hết hạn

    /*Tiến hành update các dòng cần update trong dw*/
    UPDATE datawarehouse.product_dim pd
	SET
		pd.isDelete = TRUE,				-- Đánh dấu sản phẩm là đã bị xóa
		pd.expired_date = CURRENT_DATE,	-- Cập nhật ngày hết hạn của sản phẩm thành ngày hiện tại
		pd.date_delete = CURRENT_DATE	-- Cập nhật ngày xóa sản phẩm thành ngày hiện tại
	WHERE
		pd.id IN (SELECT id FROM temp_ids);	-- Chỉ cập nhật các sản phẩm có id nằm trong bảng tạm `temp_ids`

    /*Insert các dòng có dữ liệu update vào trong dw*/
    INSERT INTO datawarehouse.product_dim(
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
        source
	)
	SELECT
		tp.product_name,
		tp.price,
		tp.image,
		length,
        width,
        height,
		tp.weight,
		tp.resolution,
		tp.sensor,
		tp.connectivity,
		tp.battery,
		tp.compatibility,
		tp.manufacturer,
        source
	FROM
		temp_update_products tp;
	/*
		Cập nhật bảng `product_dim` bằng cách liên kết với bảng `date_dim`
		và điền giá trị vào trường `date_insert_fk` trong `product_dim` dựa trên ngày tạo sản phẩm (created_at).
    */
	UPDATE product_dim AS p
	JOIN date_dim AS d ON DATE(p.created_at) = d.full_date -- Kết nối với bảng `date_dim` bằng cách so sánh ngày trong trường `created_at` của sản phẩm
    -- Cập nhật trường `date_insert_fk` trong bảng `product_dim` bằng khóa ngày (`date_sk`)
	-- từ bảng `date_dim` để liên kết sản phẩm với ngày tương ứng trong bảng `date_dim`.
    SET p.date_insert_fk = d.date_sk
    -- Chỉ cập nhật các bản ghi mà trường `date_insert_fk` trong bảng `product_dim` có giá trị NULL,
	-- đảm bảo rằng chỉ những sản phẩm chưa có thông tin ngày được cập nhật.
	WHERE p.date_insert_fk IS NULL;

end //
delimiter ;
