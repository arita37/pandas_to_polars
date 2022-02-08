import polars as pl

###################################################################################################
###### Polars #####################################################################################
def pd_split(df,  col='colstr', sep=",",  colnew="colstr_split",   mode='pandas' ):
    """
      dfp['new1'] = dfp.apply(lambda x :  min( x['str1'].split(","))   , axis=1)
      df['new1'] = df.select(["*",  pl.col("str1").apply(lambda x : min(x.split(",")) ).alias("booknew")])['booknew']

    """
    df[ colnew ] = df.select(["*",  pl.col(col).apply(lambda x : x.split(",") ).alias(colnew )])[colnew ]
    return df



def pd_groupby_join(df,  colgroup="colgroup", col='colstr', sep=",",   mode='pandas' ):
    """
      dfp['new1'] = dfp.apply(lambda x :  min( x['str1'].split(","))   , axis=1)
      df['new1']  = df.select(["*",  pl.col("str1").apply(lambda x : min(x.split(",")) ).alias("booknew")])['booknew']

    """
    if str(mode) == 'polars' :
       return df.groupby(colgroup ).agg(pl.col(col)).select([pl.col(colgroup ), pl.col(col).arr.join(sep)])
   
    return dfp.groupby([ colgroup ]).apply(lambda x: f"{sep}".join(x[ col ].values))




def test2():
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


def test_create_parquet():
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
    df.to_parquet('ztest/myfile.parquet')
    return 'ztest/myfile.parquet'

    
