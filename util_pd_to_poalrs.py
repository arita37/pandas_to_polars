import polars as pl

###################################################################################################
###### Polars #####################################################################################
def pl_split(df,  col='colstr', sep=",",  colnew="colstr_split", ):
    """
      dfp['new1'] = dfp.apply(lambda x :  min( x['str1'].split(","))   , axis=1)
      df['new1'] = df.select(["*",  pl.col("str1").apply(lambda x : min(x.split(",")) ).alias("booknew")])['booknew']

    """
    df[ colnew ] = df.select(["*",  pl.col(col).apply(lambda x : x.split(",") ).alias(colnew )])[colnew ]
    return df



def pl_groupby_join(df,  colgroup="colgroup", col='colstr', sep=",",   ):
    """
      dfp['new1'] = dfp.apply(lambda x :  min( x['str1'].split(","))   , axis=1)
      df['new1']  = df.select(["*",  pl.col("str1").apply(lambda x : min(x.split(",")) ).alias("booknew")])['booknew']

    """
    df.groupby(colgroup ).agg(pl.col(col)).select([pl.col(colgroup ), pl.col(col).arr.join(sep)])
    return df



