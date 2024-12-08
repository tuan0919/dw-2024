DELIMITER $$
DROP PROCEDURE IF EXISTS insert_file_logs;
CREATE PROCEDURE insert_file_logs(
    IN config_id INT,
    IN file_path VARCHAR(50),
    IN count INT,
    IN status VARCHAR(10),
    OUT last_id INT
)
BEGIN
    START TRANSACTION;

    INSERT INTO file_logs (config_id, create_time, file_path, count, update_at, status)
    VALUES (config_id, NOW(), file_path, count, NOW(), status);

    -- Lấy id của bản ghi vừa chèn
    SET last_id = LAST_INSERT_ID();

    COMMIT;
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS query_logs_by_id;
CREATE PROCEDURE query_logs_by_id(
    IN file_log_id INT
)
BEGIN
    START TRANSACTION;

    SELECT * FROM file_logs
    WHERE file_log_id = file_log_id;

    COMMIT;
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS update_log_status;
CREATE PROCEDURE update_log_status(
    IN log_id INT,
    IN new_status VARCHAR(10)
)
BEGIN
    START TRANSACTION;

    UPDATE file_logs
    SET
        status = new_status,
        update_at = NOW()
    WHERE
        file_log_id = log_id;

    COMMIT;
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS query_logs_to_check_crawl_execution;
CREATE PROCEDURE query_logs_to_check_crawl_execution(
    IN in_config_id INT,
    IN in_create_time DATE
)
BEGIN
    START TRANSACTION;

    SELECT * FROM file_logs
    WHERE config_id = in_config_id
    AND DATE(create_time) = in_create_time;

    COMMIT;
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS update_log_status_count_fileSize;
CREATE PROCEDURE update_log_status_count_fileSize(
    IN log_id INT,
    IN new_status VARCHAR(10),
    IN new_count INT,
    IN new_file_size INT
)
BEGIN
    START TRANSACTION;

    UPDATE file_logs
    SET
        status = new_status,
        update_at = NOW(),
        count = new_count,
        file_size = new_file_size
    WHERE
        file_log_id = log_id;

    COMMIT;
END$$
DELIMITER ;
