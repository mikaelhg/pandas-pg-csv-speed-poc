# Q: "Pandas is faster to load CSV than SQL", A: ...

## TL;DR

```bash
docker-compose up -d
docker-compose exec test pytest
```

----

https://stackoverflow.com/questions/43874559/pandas-is-faster-to-load-csv-than-sql/50809892

"Haelle" asks:

> It seems that loading data from a CSV is faster than from SQL (Postgre SQL) with Pandas. (I have a SSD)
>
> Here is my test code :

```python
import pandas as pd
import numpy as np

start = time.time()
df = pd.read_csv('foo.csv')
df *= 3
duration = time.time() - start
print('{0}s'.format(duration))

engine = create_engine('postgresql://user:password@host:port/schema')
start = time.time()
df = pd.read_sql_query("select * from mytable", engine)
df *= 3
duration = time.time() - start
print('{0}s'.format(duration))
```

> The foo.csv and the database are the same (same amount of data and columns in both, 4 columns, 100 000 rows full of random int).
>
> CSV takes 0.05s
>
> SQL takes 0.5s
>
> Do you think it's normal that CSV is 10 time faster than SQL ? I'm wondering if I'm missing something here...

-----

https://stackoverflow.com/questions/41231163/accessing-large-datasets-with-python-3-6-psycopg2-and-pandas

Nice concept by MaxU:

```python
start = time.time()
engine = create_engine(conn_str)
size = 10**4
df = pd.concat((x for x in pd.read_sql(query, engine, coerce_float=False, chunksize=size)),
               ignore_index=True)
print('time:', (time.time() - start)/60, 'minutes or ', time.time() - start, 'seconds')
```
