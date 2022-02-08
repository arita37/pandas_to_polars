### pandas_to_polars
Code snippets for polars conversion


### Colab examples

https://colab.research.google.com/drive/1Y5wKYadWzT4tLArOygem1aB8kDfQctac?usp=sharing






### Code Snippets

```python
nmin = 2
nmax=5000
# df = pd_create_random(nmax=5000000)
df = pd.DataFrame()
df['key'] = np.arange(0, nmax)
for i in range(0, nmin):
  df[ f'int{i}'] = np.random.randint(0, 100,size=(nmax, ))
  df[ f'flo{i}'] = np.random.rand(1, nmax)[0] 
  df[ f'str{i}'] =  [ ",".join([ str(t) for t in np.random.randint(10000000,999999999,size=(500, )) ] )  for k in range(0,nmax) ]
print(df.head)
df.to_parquet('myfile')


#### Pandas version
dfp = pd.read_parquet('myfile')
i=0
dfp['new1'] = dfp.apply(lambda x :  min( x['str1'].split(","))   , axis=1)
dfp.groupby([f'int{i}']).agg({'count'})
dfp.groupby(['key']).apply(lambda x:";".join(x[f'str{i}'].values))
dfp.groupby([f'flo{i}']).agg({'sum'})
dfp.to_parquet('myfile.parquet')


### POLARS Version 
df = pl.read_parquet('myfile')
i=0
df['new1'] = df.select(["*",  pl.col("str1").apply(lambda x : min(x.split(",")) ).alias("booknew")])['booknew']
df.groupby(f'int{i}').agg(pl.all().count())
df.groupby('key').agg(pl.col(f'str{i}')).select([pl.col('key'), pl.col(f'str{i}').arr.join(",")])
df.groupby([f'flo{i}']).agg(pl.all().sum())
df.to_parquet('myfile.parquet.polars')

```

---



### Code Snippets 2
```python




```
























