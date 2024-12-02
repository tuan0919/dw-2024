CREATE DEFINER=`root`@`%` PROCEDURE `insert_file_logs`(
	IN `$config_id` INT,
	IN `$file_path` VARCHAR(50),
	IN `$count` INT,
	IN `$status` VARCHAR(10),
	OUT `last_id` INT
)
LANGUAGE SQL
NOT DETERMINISTIC
CONTAINS SQL
SQL SECURITY DEFINER
COMMENT ''
BEGIN
START TRANSACTION;
INSERT INTO db_control.file_logs
(config_id, create_time, file_path, count, update_at, status)
VALUES (
           $config_id, NOW(), $file_path, $count, NOW(), $status
       );
-- Lấy id của bản ghi vừa chèn
SET last_id = LAST_INSERT_ID();
	-- Trả về bản ghi vừa được chèn
COMMIT;
END

CREATE DEFINER=`root`@`%` PROCEDURE `query_logs_by_id`(
	IN `$file_log_id` INT
)
LANGUAGE SQL
NOT DETERMINISTIC
CONTAINS SQL
SQL SECURITY DEFINER
COMMENT ''
BEGIN
START TRANSACTION;
SELECT * FROM file_logs
WHERE file_log_id = $file_log_id;
COMMIT;
END

CREATE DEFINER=`root`@`%` PROCEDURE `update_log_status`(
	IN `log_id` INT,
	IN `new_status` VARCHAR(10)
)
LANGUAGE SQL
NOT DETERMINISTIC
CONTAINS SQL
SQL SECURITY DEFINER
COMMENT ''
BEGIN
START TRANSACTION;
UPDATE file_logs
SET
    status = new_status,
    update_at = NOW()
WHERE
    file_log_id = log_id;
COMMIT;
END
