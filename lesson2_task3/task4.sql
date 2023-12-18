ADD FILE ./task4.sh;

USE odorovju;

SELECT TRANSFORM(ip, dat, url, page_size, status, browser)
USING './task4.sh' AS ip, dat, url, page_size, status, browser
FROM logs
LIMIT 10;
