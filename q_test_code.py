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
