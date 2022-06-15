o	Проект:
-
Построение через Airflow пайплайна выгрузки ежедневных отчётов по количеству поездок на велосипедах в городе Нью-Йорк. 
Проект включал анализ и оценку данных, проектирование DWH, настройку отслеживания появления новых файлов в облачном 
хранилище, запуск импорта данных в основную базу, формирование ежедневных отчетов по заданным параметрам, загрузку данных статистики 
в специальный бакет. 

Использовались Airflow, ClickHouse, YandexCloud, Python, SQL, Docker

[DAG](https://github.com/Lerozba/Portfolio/blob/main/ETL%5CELT/dag_clickhouse_etl_new.py)

[Подробное описание](https://github.com/Lerozba/Portfolio/blob/main/ETL%5CELT/Description_ELT_Airflow.pdf) 

o	Проект:
-
Реализация ETL процесса автоматизации обновления таблицы. Создана трансформация для автоматизации обновления таблицы и 
разработан ETL процесс. 

Использовались Pentaho DI, DBeaver

[Скрин ETL процесса](https://github.com/Lerozba/Portfolio/blob/main/ETL%5CELT/Description_ETL_update_process.pdf)

o	Проект:
-
Реализация ETL процесса проверки новых записей с логированием не успешных строк в отдельную таблицу. Составлен список 
проверок на качество данных и разработан ETL процесс. 

Использовались Pentaho DI, DBeaver

[Скрин ETL процесса](https://github.com/Lerozba/Portfolio/blob/main/ETL%5CELT/Description_ETL_checking_new_records.pdf)

o	Проект:
-
Работа с данными по тикетам, проставленным пользователями (получение запрошенных данных) 

Использовались Pentaho DI, DBeaver

[Скрин процессов](https://github.com/Lerozba/Portfolio/blob/main/ETL%5CELT/Description_working_with_users_tickets.pdf)

o Проект:
-
Написать скрипт запуска основного задания из консоли. В качестве основного задания взять задание, состоящее из 2х трансформаций:
1-ая трансформация загружает данные из books.csv в таблицу stage.books, 2-ая трансформация загружает данные из rocid.xml в таблицу stage.cities

Использовались Pentaho DI, DBeaver

[Описание процессов](https://github.com/Lerozba/Portfolio/blob/main/ETL%5CELT/Description_run_job_from_console.pdf)

