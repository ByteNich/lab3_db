#laba 3

Лабораторная работа №3 по базам данных
Описание проекта:
Проект включает два типа данных: tiny и big, размеры которых составляют 115 МБ и 697 МБ соответственно. В каждом из пяти файлов на Python реализованы четыре SQL-запроса для каждой базы данных (tiny и big), используя конкретные библиотеки.


SQL-запросы:
SELECT VendorID, count(*) FROM trips GROUP BY 1;
SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;
SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;
SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;
Впечатления от библиотек:
Наиболее привлекательной оказалась библиотека duckdb из-за своей простоты в использовании и высокой производительности. Наименее удобной была библиотека pandas из-за специфичного синтаксиса, который кажется сложнее по сравнению с другими библиотеками.

Графики:
1) на маленьких данных

![image](https://imgur.com/EfXL5Su)
https://imgur.com/EfXL5Su
![image](https://imgur.com/ovvcMfW)
https://imgur.com/ovvcMfW

2) на больших данных

![image](https://imgur.com/HTezd2a)
https://imgur.com/HTezd2a
![image](https://imgur.com/oZflaGs)
https://imgur.com/oZflaGs

Анализ графиков:
Библиотека duckdb оказалась самой быстрой на всех запросах и данных любого размера. Это объясняется тем, что duckdb использует векторизацию, ориентированную на столбцы, в то время как SQLite, PostgreSQL и другие обрабатывают каждую строку последовательно. Самыми медленными библиотеками оказались SQLite и SQLAlchemy, их время работы приблизительно одинаково, поскольку SQLAlchemy реализована поверх SQLite."# lab3_db" 
