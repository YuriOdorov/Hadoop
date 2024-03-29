## Исходные данные: логи пользователей

Данные находятся в HDFS по адресу `/data/user_logs/*_M`. Они состоят из трёх частей, каждая из которых находится в своей поддиректории. Данные в каждой части отличаются количеством и типом колонок, разделенных знаками табуляции ('\t') или пробелами.

#### А. Логи запросов пользователей к новостным сообщениям (user_logs).
1. Ip-адрес, с которого пришел запрос (STRING),
2. Время запроса (TIMESTAMP или INT),
3. Пришедший с ip-адреса http-запрос (STRING),
4. Размер переданной клиенту страницы (INT),
5. Http-статус код (INT).
6. Информация о клиентском приложении, с которого осуществлялся запрос на сервер, в том числе, информация о браузере (STRING).

**Важно:** информация о браузере содержится в начале 6-ого поля лога (символы с нулевой позиции до позиции первого пробельного символа), содержание оставшейся части строки не определяет браузер пользователя. Разделитель между IP и временем запроса имеет 3 табуляции.

#### B. Информация о пользователях (user_data).
1. IP-адрес (STRING),
2. Браузер пользователя (STRING),
3. Пол (STRING) //male, female,
4. Возраст (INT).

#### С. Информация о местонахождении IP адресов пользователей (ip_data).
1. IP-адрес (STRING),
2. Регион (STRING).

## Задача


**Задача**. Напишите запрос, выбирающий количество посещений для каждого дня. Полученные результаты отсортируйте по убыванию количества.

*Пример результата:*
```
20140308	96
20140409	96
20140318	96
```
Т.к. после агрегации данных становится немного, `LIMIT` использовать не надо.
