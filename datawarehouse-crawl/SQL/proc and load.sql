
/*========================================================================================================================*/
/*PROC CLEANING, TRANSFROM AND SPIT TABLE FROM STAGING TO DW*/
use dbstaging;
delimiter //
CREATE PROCEDURE LoadDataFromStagingToDW()
BEGIN
    START TRANSACTION;

    -- Drop temporary tables if they exist
    DROP TEMPORARY TABLE IF EXISTS temp_update_products;
    DROP TEMPORARY TABLE IF EXISTS temp_product;
	DROP TEMPORARY TABLE IF EXISTS temp_ids;
    DROP TEMPORARY TABLE IF EXISTS temp_ids_to_update;
    -- Insert date_dim
    INSERT INTO datawarehouse.date_dim (
		date_sk,
		full_date,
		day_since_2005,
		month_since_2005,
		day_of_week,
		calendar_month,
		calendar_year,
		calendar_year_month,
		day_of_month,
		day_of_year,
		week_of_year_sunday,
		year_week_sunday,
		week_sunday_start,
		week_of_year_monday,
		year_week_monday,
		week_monday_start,
		holiday,
		day_type
	)
	SELECT 
		date_sk,
		full_date,
		day_since_2005,
		month_since_2005,
		day_of_week,
		calendar_month,
		calendar_year,
		calendar_year_month,
		day_of_month,
		day_of_year,
		week_of_year_sunday,
		year_week_sunday,
		week_sunday_start,
		week_of_year_monday,
		year_week_monday,
		week_monday_start,
		holiday,
		day_type
	FROM dbstaging.date_dim as ds
    WHERE NOT EXISTS (
    SELECT 1 
    FROM datawarehouse.date_dim AS dw
    WHERE dw.date_sk = ds.date_sk
	);


    -- Xóa các bản ghi không có tên product_name like 'None'
	CREATE TEMPORARY TABLE temp_ids AS 
	SELECT id FROM dbstaging.product_dim WHERE product_name LIKE 'None';

	DELETE FROM dbstaging.product_dim
	WHERE EXISTS (
		SELECT 1
		FROM temp_ids
		WHERE temp_ids.id = product_dim.id
	);


	DROP TEMPORARY TABLE temp_ids;


    -- Tạo bảng tạm lưu product
    CREATE TEMPORARY TABLE temp_product AS
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_name, manufacturer, size, weight, resolution ORDER BY manufacturer) AS row_num
        FROM dbstaging.product_dim
    ) AS temp
    WHERE row_num = 1;
    
	ALTER TABLE temp_product
	CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

    -- Tách bảng + check trùng + check update - manufacturer
    INSERT IGNORE INTO datawarehouse.manufacturer_dim(manufacturer_name)
	SELECT DISTINCT tp.manufacturer
	FROM temp_product tp
	WHERE tp.manufacturer NOT LIKE 'None'
    AND NOT EXISTS (
        SELECT 1 
        FROM datawarehouse.manufacturer_dim md 
        WHERE tp.manufacturer = md.manufacturer_name
    );

    -- product
    INSERT INTO datawarehouse.product_dim (
    product_name, image, size, weight, resolution, sensor, button, 
    connectivity, battery, compatibility, utility, manufacturer_id
	)
	SELECT 
		tp.product_name, tp.image, tp.size, tp.weight, tp.resolution, 
		tp.sensor, tp.button, tp.connectivity, tp.battery, tp.compatibility, 
		tp.utility, md.id AS manufacturer_id
	FROM temp_product tp
	JOIN datawarehouse.manufacturer_dim md 
		ON tp.manufacturer = md.manufacturer_name
	WHERE NOT EXISTS (
		SELECT 1 
		FROM datawarehouse.product_dim pd
		WHERE pd.product_name = tp.product_name
		AND pd.manufacturer_id = md.id
	);

    -- Tạo bảng tạm để lưu các sản phẩm cần cập nhật
    CREATE TEMPORARY TABLE temp_update_products AS
    SELECT tp.*, md.id AS manufacturer_id
    FROM temp_product tp
    JOIN datawarehouse.manufacturer_dim md ON tp.manufacturer = md.manufacturer_name
    WHERE EXISTS (
        SELECT 1
        FROM datawarehouse.product_dim pd2
        WHERE pd2.product_name = tp.product_name
        AND pd2.isDelete = FALSE
        AND pd2.expired_date = '9999-12-31'
        and pd2.manufacturer_id = md.id
        AND (pd2.image <> tp.image OR
             pd2.size <> tp.size OR
             pd2.weight <> tp.weight OR
             pd2.resolution <> tp.resolution OR
             pd2.sensor <> tp.sensor OR
             pd2.button <> tp.button OR
             pd2.connectivity <> tp.connectivity OR
             pd2.battery <> tp.battery OR
             pd2.compatibility <> tp.compatibility OR
             pd2.utility <> tp.utility)
    );
    -- Cập nhật trạng thái của các sản phẩm trong bảng datawarehouse
	CREATE TEMPORARY TABLE temp_ids AS
	SELECT pd2.id
	FROM datawarehouse.product_dim pd2
	JOIN temp_update_products tup ON tup.product_name = pd2.product_name 
		AND tup.manufacturer_id = pd2.manufacturer_id
	WHERE 
		pd2.isDelete = FALSE 
		AND pd2.expired_date = '9999-12-31';
        -- ==================== 
        
	UPDATE datawarehouse.product_dim pd
	SET 
		pd.isDelete = TRUE,
		pd.expired_date = CURRENT_DATE,
		pd.date_delete = CURRENT_DATE
	WHERE 
		pd.id IN (SELECT id FROM temp_ids);
	DROP TEMPORARY TABLE IF EXISTS temp_ids;

    -- Insert các dòng update
    INSERT INTO datawarehouse.product_dim(
    product_name, image, size, weight, resolution, sensor, button, 
    connectivity, battery, compatibility, utility, manufacturer_id
	)
	SELECT 
		tp.product_name, 
		tp.image, 
		tp.size, 
		tp.weight, 
		tp.resolution, 
		tp.sensor, 
		tp.button, 
		tp.connectivity, 
		tp.battery, 
		tp.compatibility, 
		tp.utility, 
		md.id AS manufacturer_id
	FROM 
		temp_update_products tp
	JOIN 
		datawarehouse.manufacturer_dim md ON tp.manufacturer = md.manufacturer_name;

    COMMIT;
END //
delimiter ;


CALL LoadDataFromStagingToDW();
drop procedure dbstaging.LoadDataFromStagingToDW;
select database();

SHOW PROCEDURE STATUS WHERE Db = 'dbstaging';
select * from datawarehouse.product_dim order by id;
select count(*) from datawarehouse.product_dim;
select * from datawarehouse.manufacturer_dim order by id;
select * from dbstaging.product_dim;
truncate table datawarehouse.product_dim;
truncate table datawarehouse.manufacturer_dim;
truncate table datawarehouse.product_dim;
drop table temp_product;
drop table temp_update_products;

SELECT AUTO_INCREMENT FROM information_schema.tables 
WHERE table_name = 'manufacturer_dim' AND table_schema = 'datawarehouse';



/*===================================================TEST============================================================*/
select * from temp_product order by id;


select count(*) from temp_product ;
select count(*) from product_dim;
select count(*) from datawarehouse.product_dim;
 -- Kiểm tra bộ dữ liệu nào bị trùng - test chứ không đưa vào proc
/*
WITH RankedProducts AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_name, manufacturer, size, weight, resolution ORDER BY id) AS row_num
    FROM dbstaging.product_dim
)
SELECT f.id AS filtered_product_id, 
       f.product_name AS filtered_product_name, 
       f.manufacturer AS filtered_manufacturer, 
       f.size AS filtered_size,
       k.id AS kept_product_id, 
       k.product_name AS kept_product_name, 
       k.manufacturer AS kept_manufacturer, 
       k.size AS kept_size
FROM RankedProducts f
JOIN RankedProducts k 
  ON f.manufacturer = k.manufacturer 
     AND f.size = k.size 
     AND f.weight = k.weight 
     AND f.resolution = k.resolution
WHERE f.row_num > 1 AND k.row_num = 1;

*/
SET SQL_SAFE_UPDATES = 0;
select count(*) from datawarehouse.product_dim;
select count(*) from datawarehouse.manufacturer_dim;
select * from datawarehouse.manufacturer_dim;
