use datamart;
drop procedure if exists load_from_dw_to_dm;
call load_from_dw_to_dm
DELIMITER //
CREATE PROCEDURE load_from_dw_to_dm()
BEGIN
    -- Cập nhật dữ liệu đã thay đổi từ DW vào DM
    UPDATE product_dim dm
    JOIN datawarehouse.product_dim dw ON  dm.product_name = dw.product_name     
    SET dm.id = dw.id,
        dm.price = dw.price,
        dm.image = dw.image,
        dm.weight = dw.weight,
        dm.resolution = dw.resolution,
        dm.sensor = dw.sensor,
        dm.connectivity = dw.connectivity,
        dm.battery = dw.battery,
        dm.compatibility = dw.compatibility,
        dm.length = dw.length,
        dm.width = dw.width,
        dm.height = dw.height
    WHERE dw.isDelete = FALSE
		AND   dm.manufacturer = dw.manufacturer
        AND (dw.price <> dm.price OR					
             dw.image <> dm.image OR					
             dw.length <> dm.length OR					
			 dw.width <> dm.width OR					
			 dw.height <> dm.height OR					
             dw.weight <> dm.weight OR					
             dw.resolution <> dm.resolution OR			
             dw.sensor <> dm.sensor OR					
             dw.connectivity <> dm.connectivity OR		
             dw.battery <> dm.battery OR				
             dw.compatibility <> dm.compatibility OR	
             dw.manufacturer <> dm.manufacturer);
    
    -- Chèn dữ liệu mới từ DW vào DM
    INSERT INTO product_dim (
        id, 
        product_name, 
        price, 
        image, 
        weight, 
        resolution, 
        sensor, 
        connectivity, 
        battery, 
        compatibility, 
        manufacturer, 
        length, 
        width, 
        height
    )
    SELECT 
        id, 
        product_name, 
        price, 
        image, 
        weight, 
        resolution, 
        sensor, 
        connectivity, 
        battery, 
        compatibility, 
        manufacturer, 
        length, 
        width, 
        height
    FROM 
        datawarehouse.product_dim dw
    WHERE 
        dw.product_name NOT IN (SELECT product_name FROM datamart) AND dw.isDelete = FALSE;
    -- Xóa các sản phẩm đã đánh dấu xóa trong DM 
    DELETE FROM product_dim
    WHERE id IN (
        SELECT id FROM datawarehouse.product_dim WHERE isDelete = TRUE
    );
END$$

DELIMITER ;
