# hadoop-mr-spark
Hive, MR, Spark tasks

Hive: 48.3s
Spark: 2m, 57s
MapReduce: 5m:51s

Результаты всех задач совпадают. Результаты Spark и MapReduce можно посмотреть в следующих папках:
MapReduce: https://hdp.mai.moscow/hue/filebrowser/view=%2Fuser%2Fstud#/user/stud/andrew/mapreduce/output/final
Spark: https://hdp.mai.moscow/hue/filebrowser/view=%2Fuser%2Fstud%2Fandrew%2Fspark%2Foutput

Сжатие не использовал, работал с текстовыми файлами.
Алгоритм решения:
  Таблицу постов делим на вопросы и ответы, при этом фильтруя от лишних и неправильных значений. Оставляю поля acceptedpostid и creationdate для вопросов и id, creationdate, owneruserid для ответов. Затем делаю join вопросов и ответов и считаю время между созданием вопроса и созданием правильного ответа. Затем делаю агрегацию, делаю group by по owneruserid и считаю среднее время ответа для каждого пользователя. Затем делаю join этой таблицы с юзерами и получаю имена пользователей. Затем сортирую получившиеся данные.

  В MapReduce 4 jobs:
    1 job: Posts join Posts. Джойнятся вопросы и ответы + фильтруются
    2 job: PostPost join Users + agregate. Получаем юзер id, среднее время и имя.
    3 job: Analysis. Анализ для последующей сортировки.
    4 job: Sort. Сортировка.
  
  Spark делал локально в докере. Спарк больше понравился.
