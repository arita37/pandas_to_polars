#### 1) pandas_to_polars
     Code snippets for polars conversion


#### 2) Colab examples
     https://colab.research.google.com/drive/1rgiA-HHd5w8gIHtcS8_IaL3saVZKi7bO?usp=sharing


#### 3) Stackoverflow
    https://stackoverflow.com/questions/tagged/python-polars




#### Code Snippets 1
```python
def pd_compare(df, dfp):
     key = True
     for j in dfp.columns:
       for i in range(nmax):
         if dfp[j][i] != df[j][i]:
           key = dfp[j][i], df[j][i]
           break
     print(f'Our Pandas and Palars dataframe output are same: {key}')



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

#### Code Snippet 3
```python

# make dataset
N_ROWS = 50

event_sizes = np.random.randint(1,9, size=N_ROWS)
data = {
    'timestamp': np.arange(N_ROWS, dtype=np.float16) \
                    + np.random.randn(N_ROWS)+1,
    'detector_type': np.random.choice(['MM', 'sTGC'], N_ROWS),
    'sector_id': np.random.choice(np.arange(16)+1, N_ROWS),
    'PCB_no': [np.random.choice(np.arange(4)+1, n)
                    for n in event_sizes],
    'reading': [np.random.randint(1, 1025, size=n)
                for n in event_sizes]
}

df_pd = pd.DataFrame(data)
df_pl = pl.from_pandas(df_pd)

df_pl

# pivot_table in PANDAS
# get the mean value of 'PCB_no' and 'reading' through exploding and pivoting
pivot_pd = df_pd.explode(['reading', 'PCB_no']).pivot_table(
            index=[ 'timestamp', 'detector_type', 'sector_id'], 
            values=['reading', 'PCB_no'], aggfunc=np.mean
        ).sort_values('timestamp').reset_index()
        
# get the total number of readings by detector type
readings_by_type_pd = df_pd.explode(['reading', 'PCB_no']).groupby('detector_type')[['reading']].count()\
    .rename(columns={'reading':'reading_count'}).sort_index()
    
# alternatives in POLARS
# there is no `pivot_table` in polars
# we use `melt` and `pivot` instead
pivot_pl = df_pl.explode(['reading', 'PCB_no']).melt(
                id_vars=['timestamp', 'detector_type', 'sector_id'], 
                value_vars=['PCB_no','reading']
            ).pivot(index=[ 'timestamp','detector_type', 'sector_id'], 
                columns='variable', values='value', 
                aggregate_fn='mean') \
        .sort('timestamp')

# for the readings by type we can use the same pandas syntax (sort of)
# this comes with the price of a DeprecationWarning :(
readings_by_type_pl = df_pl.explode(['reading', 'PCB_no']).groupby(
                "detector_type", maintain_order=True
                )[['reading']].count()

```









