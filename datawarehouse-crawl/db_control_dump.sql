-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Server version:               8.0.19 - MySQL Community Server - GPL
-- Server OS:                    Linux
-- HeidiSQL Version:             12.8.0.6908
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- Dumping database structure for db_control
DROP DATABASE IF EXISTS `db_control`;
CREATE DATABASE IF NOT EXISTS `db_control` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `db_control`;

-- Dumping data for table db_control.configs: ~0 rows (approximately)
DELETE FROM `configs`;
INSERT INTO `configs` (`config_id`, `save_location`, `tble_warehouse`, `tble_staging`, `url`, `file_name`, `schedule`, `staging_fields`, `dw_fields`, `fields_terminated_by`, `optionally_enclosed_by`, `lines_terminated_by`, `ignore_rows`, `staging_table`, `property_id`) VALUES
	(1, 'E:\\Temp', 'warehouse_mouse', 'staging_mouse_daily', 'https://cellphones.com.vn/phu-kien/chuot-ban-phim-may-tinh/chuot.html', 'crawl_cellphones', 8600, 'weight, size, dpi, sensor, connector, pin, os, brand, name, price, images', NULL, NULL, NULL, NULL, NULL, 'staging_mouse_cellphones', 1);

-- Dumping data for table db_control.file_logs: ~0 rows (approximately)
DELETE FROM `file_logs`;
INSERT INTO `file_logs` (`file_log_id`, `config_id`, `time`, `file_path`, `count`, `file_size`, `update_at`, `status`) VALUES
	(6, 1, '04-11-2024', 'E:\\Temp/crawl_cellphones_04-11-2024.csv', 0, NULL, '2024-11-03 21:55:48', 'C_SE');

-- Dumping data for table db_control.process_properties: ~0 rows (approximately)
DELETE FROM `process_properties`;
INSERT INTO `process_properties` (`property_id`, `name`, `header_csv`, `value`, `last_modified`) VALUES
	(1, 'cellphone_properties', 'weight, size, dpi, sensor, connector, pin, os, brand, name, price, images', '{\r\n  "instance": "com.nlu.app.entity.Mouse_Cellphones.class",\r\n  "crawler": "com.nlu.app.service.crawler.Crawler_Cellphones.class",\r\n  "extractor": "com.nlu.app.service.extractor.Extractor_Cellphones.class",\r\n  "fields": [\r\n    {\r\n      "field_name": "weight",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Trọng lượng\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "size",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Kích thước\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "dpi",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Độ phân giải\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "sensor",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Cảm biến\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "connector",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Kết nối\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "pin",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Pin\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "os",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Tương thích\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "brand",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "xpath",\r\n      "selector": "//li//p[text()=\'Hãng sản xuất\']/following-sibling::div[1]"\r\n    },\r\n    {\r\n      "field_name": "name",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "css",\r\n      "selector": ".box-product-name h1"\r\n    },\r\n    {\r\n      "field_name": "price",\r\n      "number": "single",\r\n      "attr": "text",\r\n      "type": "css",\r\n      "selector": ".product__price--show"\r\n    },\r\n    {\r\n      "field_name": "images",\r\n      "number": "multiple",\r\n      "attr": "src",\r\n      "type": "css",\r\n      "selector": ".swiper-wrapper img"\r\n    }\r\n  ]\r\n}', '2024-11-03 17:52:05');

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
