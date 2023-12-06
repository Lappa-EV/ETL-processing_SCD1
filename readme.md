## Представленный ETL-процесс выполнен в форме SCD1
[Задание на построение ETL процесса](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/task_of_building_an_ETL_process.pdf)
## Описание:
По легенде ежедневно некие информационные системы выгружают три следующих файла: 
1. Список транзакций за текущий день. Формат – CSV (3 шт.)
- [transactions_01032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/transactions_01032021.txt)
- [transactions_02032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/transactions_02032021.txt)
- [transactions_03032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/transactions_03032021.txt)

2.	Список терминалов полным срезом. Формат – XLSX (3 шт.)
- [terminals_01032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/terminals_01032021.xlsx)
- [terminals_02032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/terminals_02032021.xlsx)
- [terminals_03032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/terminals_03032021.xlsx)
3.	Список паспортов, включенных в «черный список» - с накоплением с начала месяца (3 шт.)
- [passport_blacklist_01032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/passport_blacklist_01032021.xlsx)
- [passport_blacklist_02032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/passport_blacklist_02032021.xlsx)
- [passport_blacklist_03032021](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/passport_blacklist_03032021.xlsx)
4.	Сведения о картах, счетах и клиентах хранятся в СУБД PostgreSQL.
---
По заданию необходимо ежедневно строить витрину отчетности по мошенническим операциям. 

Признаки мошеннических операций: 
1.	Совершение операции при просроченном или заблокированном паспорте. 
2.	Совершение операции при недействующем договоре. 
3.	Совершение операций в разных городах в течение одного часа. 

---

При разработке ETL процесса на сервере были созданы необходимые каталоги: 
- ~/lapp/project – папка с проектом 
- ~/lapp/project/archive – каталог для архивации считанных файлов

и файлы: 
- [ ~/lapp/project/main.py](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/main.py) – исполняемый файл с основным кодом ETL-процесса
- [ ~/lapp/project/main.ddl](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/main.ddl) – файл со SQL-кодом создания таблиц 
- [ ~/lapp/project/main.cron](https://github.com/Lappa-EV/ETL-processing_SCD1/blob/main/main.cron) – файл для ежедневного запуска ETL-процесса
---

### Создание данного ETL-процесса включает в себя основных 6 этапов:
1. Создание таблиц стейджинга и детального слоя, а также мета-таблицы
2. Подготовка данных и захват данных из источника в стейджинг
3. Предварительная очистка стейджинга
4. Загрузка данных в стейджинговый слой
5. Загрузка данных в детальный слой
6. Построение витрин отчетности
