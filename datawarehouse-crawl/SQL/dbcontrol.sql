create database if not exists dbcontrol;
use dbcontrol;


CREATE TABLE if not exists process_properties (
    property_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    header_csv text,
    value text,
    last_modified DATETIME
);

CREATE TABLE if not exists configs (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    save_location VARCHAR(255),
    tble_warehouse VARCHAR(255),
    tble_staging VARCHAR(255),
    url VARCHAR(255),
    file_name VARCHAR(255),
    schedule BIGINT,
    staging_fields TEXT,
    dw_fields TEXT,
    fields_terminated_by VARCHAR(10),
    optionally_enclosed_by VARCHAR(10),
    lines_terminated_by VARCHAR(10),
    ignore_rows INT,
    staging_table VARCHAR(255),
    property_id INT
);


CREATE TABLE if not exists file_logs (
    file_log_id INT AUTO_INCREMENT PRIMARY KEY,
    config_id INT,
    time VARCHAR(255),
    file_path VARCHAR(255),
    count INT,
    start_time DATETIME,
    end_time DATETIME,
    file_size DECIMAL(10, 2),
    update_at DATETIME,
    status ENUM('C_RE', 'C_E', 'C_SE', 'C_FE', 'L_RE', 'L_P', 'L_SE', 'L_FE', 'L_CE') NOT NULL
);


-- Dữ liệu mẫu

INSERT INTO `process_properties` ( `name`, `header_csv`, `value`, `last_modified`) VALUES
	('cellphone_properties', 'weight, size, dpi, sensor, connector, pin, os, brand, name, price, images', '{\r\n  "instance": "com.nlu.app.entity.Mouse_Cellphones.class",\r\n  "crawler": "com.nlu.app.service.crawler.Crawler_Cellphones.class",\r\n  "extractor": "com.nlu.app.service.extractor.Extractor_Cellphones.class",\r\n  "fields": [\r\n    {\r\n      "field_name": "weight",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Trọng lượng\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "size",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Kích thước\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "dpi",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Độ phân giải\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "sensor",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Cảm biến\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "connector",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Kết nối\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "pin",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Pin\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "os",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Tương thích\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "brand",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Hãng sản xuất\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "name",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "css",\r\n      "selector": ".box-product-name h1"\r\n    },\r\n    {\r\n      "field_name": "price",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "css",\r\n      "selector": ".product__price--show"\r\n    },\r\n    {\r\n      "field_name": "images",\r\n      "number": "multiple",\r\n      "attr": "src",\r\n      "type": "css",\r\n      "selector": ".swiper-wrapper img"\r\n    }\r\n  ]\r\n}', '2024-11-03 17:52:05');
INSERT INTO `process_properties` ( `name`, `header_csv`, `value`, `last_modified`) VALUES
	('gearvn_properties', 'weight, brand, dpi, size, sensor, connector, os, pin, images, name, price', '{\r\n  "instance": "com.nlu.app.entity.Mouse_Cellphones.class",\r\n  "crawler": "com.nlu.app.service.crawler.Crawler_Cellphones.class",\r\n  "extractor": "com.nlu.app.service.extractor.Extractor_Cellphones.class",\r\n  "fields": [\r\n    {\r\n      "field_name": "weight",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Trọng lượng\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "size",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Kích thước\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "dpi",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Độ phân giải\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "sensor",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Cảm biến\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "connector",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Kết nối\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "pin",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Pin\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "os",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Tương thích\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "brand",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Hãng sản xuất\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "name",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "css",\r\n      "selector": ".box-product-name h1"\r\n    },\r\n    {\r\n      "field_name": "price",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "css",\r\n      "selector": ".product__price--show"\r\n    },\r\n    {\r\n      "field_name": "images",\r\n      "number": "multiple",\r\n      "attr": "src",\r\n      "type": "css",\r\n      "selector": ".swiper-wrapper img"\r\n    }\r\n  ]\r\n}', '2024-11-03 17:52:05');


INSERT INTO `configs` (`save_location`, `tble_warehouse`, `tble_staging`, `url`, `file_name`, `schedule`, `staging_fields`, `dw_fields`, `fields_terminated_by`, `optionally_enclosed_by`, `lines_terminated_by`, `ignore_rows`, `staging_table`, `property_id`) VALUES
	('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads', 'warehouse_mouse', 'staging_mouse_daily', 'https://cellphones.com.vn/phu-kien/chuot-ban-phim-may-tinh/chuot.html', 'crawl_cellphones', 8600, 'weight, size, dpi, sensor, connector, pin, os, brand, name, price, images', 'weight, size, dpi, sensor, connector, pin, os, brand, name, price, images', ',', '"', '\\n', 1, 'staging_mouse_cellphones', 1);
INSERT INTO `configs` (`save_location`, `tble_warehouse`, `tble_staging`, `url`, `file_name`, `schedule`, `staging_fields`, `dw_fields`, `fields_terminated_by`, `optionally_enclosed_by`, `lines_terminated_by`, `ignore_rows`, `staging_table`, `property_id`) VALUES
	('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads', 'warehouse_mouse', 'staging_mouse_daily', 'https://cellphones.com.vn/phu-kien/chuot-ban-phim-may-tinh/chuot.html', 'crawl_cellphones', 8600, 'weight, size, dpi, sensor, connector, pin, os, brand, name, price, images', 'weight, size, dpi, sensor, connector, pin, os, brand, name, price, images', ',', '"', '\\n', 1, 'staging_gearvn', 2);


	INSERT INTO `file_logs` (`config_id`, `time`, `file_path`, `count`,  `start_time`,  `end_time`, `file_size`, `update_at`, `status`) VALUES
		(1, '07-11-2024', 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/crawl_cellphones_04-11-20241.csv', 0, NULL, NULL, NULL, '2024-11-12 21:55:48', 'C_SE');
INSERT INTO `file_logs` (`config_id`, `time`, `file_path`, `count`,  `start_time`,  `end_time`, `file_size`, `update_at`, `status`) VALUES
		(2, '07-11-2024', 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/crawl_gearvn_12-11-2024.csv', 0, NULL, NULL, NULL, '2024-11-12 21:55:48', 'C_SE');

