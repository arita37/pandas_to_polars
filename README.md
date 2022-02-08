# pandas_to_polars
Code snippets for polars conversion


## Colab examples


https://colab.research.google.com/drive/1Y5wKYadWzT4tLArOygem1aB8kDfQctac?usp=sharing




## Code Snippets



```python
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







