#### 1) pandas_to_polars
     Code snippets for polars conversion


#### 2) Colab examples
     https://colab.research.google.com/drive/1Y5wKYadWzT4tLArOygem1aB8kDfQctac?usp=sharing


#### 3) Stackoverflow
    https://stackoverflow.com/questions/tagged/python-polars




#### Code Snippets 1
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



#### Code Snippets 2
```python
LINES = 10000
districts = np.random.choice(['A', 'B', 'C', 'D'], size=LINES, replace=True)
space = np.random.randint(1000, size=LINES)
price = np.random.randint(1000, size=LINES)
data = {
    "District": districts,
    "Space": space,
    "Price": price,
}


def pandas_test():
  df = pd.DataFrame(data)
  agg_df = df.groupby(["District"]).agg([min, np.average])
  
  apply_df = df.groupby("District").apply(lambda d: d.Space.sum() / d.Price.sum())
  
  
  df.to_parquet('pandas1.parquet')
  loaded_df = pd.read_parquet('pandas1.parquet')

  return df, agg_df, apply_df, loaded_df


def polars_test():
  df = pl.DataFrame(data)
  agg_df = df.groupby("District", maintain_order=True).agg([pl.mean("*"), pl.min("*")])
  
  apply_df = df.groupby("District", maintain_order=True).agg(
    pl.apply( f=lambda spacePrice: spacePrice[0].sum() / spacePrice[1].sum(),
              exprs=["Space", "Price"]
        )
  )
  df.to_parquet('polars1.parquet')
  loaded_df = pl.read_parquet('polars1.parquet')

  return df, agg_df, apply_df, loaded_df

pandas_result = pandas_test()
polars_result = polars_test()


print("Pandas:")
%timeit pandas_test()

print("Polars:")
%timeit polars_test()


```











