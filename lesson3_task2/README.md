## Задача

#### Исходные данные
* Полный датасет: `/data/twitter/twitter_sample.txt` (при коммите в систему указывайте в коде этот датасет)
* Частичная выборка: `/data/twitter/twitter_sample_small.txt`
**Формат данных:**
```
follower_id \t user_id
```
#### Условие
Дан ориентированный граф.Необходимо найти длину кратчайшего пути между  вершинами 12 и 34 графа, реализовав алгоритм "Поиск в ширину". Если кратчайших путей несколько, выведите первый.
Обратите внимание на критерий остановки алгоритма. В рамках оптимизации вы можете остановить программу раньше, чем закончится поиск в ширину т.к. нам достаточно одно пути. 
Выходной формат: последовательность вершин (учитывая начало и конец), разделенных запятой, без пробелов. Например, путь «12 -> 42 -> 34» должен быть напечатан как: 12,42,34.

#### Дополнительные комментарии
Данных немного поэтому есть соблазн на каком-нибудь этапе решения сделать `take()` или `collect()`, сконвертировав RDD в обычный Python-объект. Конечно, с точки зрения API, работать с обычными объектами привычнее. Но т.к. обычные объекты из коробки не отвечают требованиям высокодоступности и распределённости, такое решение учитываться не будет.
Помните, что по возможности необходимо избегать написания UDF, вместо этого внимательно изучите возможности pyspark.sql.functions. Вам точно пригодится этот модуль.

#### Стартовый фрагмент кода
От этого фрагмента кода можно отталкиваться при решении задачи. Этот код не эффективный поэтому он не будет работать в системе проверки. Его цель - дать понимание, от чего отталкиваться в задаче.
```python
def parse_edge(s):
  user, follower = s.split("\t")
  return (int(user), int(follower))

def step(item):
  prev_v, prev_d, next_v = item[0], item[1][0], item[1][1]
  return (next_v, prev_d + 1)

def complete(item):
  v, old_d, new_d = item[0], item[1][0], item[1][1]
  return (v, old_d if old_d is not None else new_d)

n = 400  # number of partitions
edges = sc.textFile("/data/twitter/twitter_sample_small.txt").map(parse_edge).cache()
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist()

x = 12
d = 0
distances = sc.parallelize([(x, d)]).partitionBy(n)
while True:
  candidates = distances.join(forward_edges, n).map(step)
  new_distances = distances.fullOuterJoin(candidates, n).map(complete, True).persist()
  count = new_distances.filter(lambda i: i[1] == d + 1).count()
  if count > 0:
    d += 1
    distances = new_distances
  else:
    break
```
Код для создания SparkContext
```python
from pyspark import SparkContext, SparkConf

config = SparkConf().setAppName("my_super_app").setMaster("local[3]")  # конфиг, в котором указываем название приложения и режим выполнения (local[*] для локального запуска, yarn для запуска через YARN).
sc = SparkContext(conf=config)  # создаём контекст, пользуясь конфигом
```