use datawarehouse;
/*
Viết proc xử lý quá trình load từ staging sang dw
	1. Kiểm tra xem dữ liệu khi insert vào dw có hoàn toàn mới hay không. Nếu có -> insert
    2. Nếu dữ liệu được cập nhật -> B1: Thay đổi dòng dữ liệu trong dw với: isDelete = True, expried_date = NOW(), date_delete = NOW()
									B2: Insert dòng dữ liệu có thay đổi vào dw
*/
drop procedure if exists load_from_staging_to_dw;
delimiter //
create procedure load_from_staging_to_dw()
begin
	DROP temporary TABLE IF  EXISTS temp_update_products;
    DROP temporary TABLE IF  EXISTS temp_ids;
	/*Insert các dòng mới*/
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
		manufacturer
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
		manufacturer
	FROM dbstaging.staging_mouse_daily sm
	WHERE NOT EXISTS (
		SELECT 1 
		FROM datawarehouse.product_dim pd
		WHERE pd.product_name = sm.product_name
		AND pd.manufacturer = sm.manufacturer
	);
    
    
    /*Update các dòng có thay đổi*/
    CREATE TEMPORARY TABLE temp_update_products AS
    SELECT sm.*
    FROM dbstaging.staging_mouse_daily sm
    WHERE EXISTS (
        SELECT 1
        FROM datawarehouse.product_dim pd2
        WHERE pd2.product_name = sm.product_name
        AND pd2.isDelete = FALSE
        AND pd2.expired_date = '9999-12-31'
        and pd2.manufacturer = sm.manufacturer
        AND (pd2.price <> sm.price OR
             pd2.image <> sm.image OR
             pd2.length <>sm.length or
			 pd2.width <> sm.width or
			 pd2.height <> sm.height or
             pd2.weight <> sm.weight OR
             pd2.resolution <> sm.resolution OR
             pd2.sensor <> sm.sensor OR
             pd2.connectivity <> sm.connectivity OR
             pd2.battery <> sm.battery OR
             pd2.compatibility <> sm.compatibility OR
             pd2.manufacturer <> sm.manufacturer)
    );
    
    /*Tạo bảng tạm để lưu id các sản phẩm cần update thông tin được lấy từ bảng temp_update_products*/
    CREATE TEMPORARY TABLE temp_ids AS
	SELECT pd2.id
	FROM datawarehouse.product_dim pd2
	JOIN temp_update_products tup ON tup.product_name = pd2.product_name 
		AND tup.manufacturer = pd2.manufacturer
	WHERE 
		pd2.isDelete = FALSE 
		AND pd2.expired_date = '9999-12-31';
    
    /*Tiến hành update các dòng cần update trong dw*/
    UPDATE datawarehouse.product_dim pd
	SET 
		pd.isDelete = TRUE,
		pd.expired_date = CURRENT_DATE,
		pd.date_delete = CURRENT_DATE
	WHERE 
		pd.id IN (SELECT id FROM temp_ids);
	DROP TEMPORARY TABLE IF EXISTS temp_ids;
    
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
		manufacturer
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
		tp.manufacturer
	FROM 
		temp_update_products tp;

    
    
end //
delimiter ;

