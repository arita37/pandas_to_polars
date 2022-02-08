

### Code Snippets 3
<table>
<tr><th> Pandas </th> <th> Polars </th>
</tr><tr><td>

```python
  agg_df = df.groupby(["District"]).agg([min, np.average])
  
  apply_df = df.groupby("District").apply(lambda d: d.Space.sum() / d.Price.sum())
  
  df.to_parquet('pandas1.parquet')
  loaded_df = pd.read_parquet('pandas1.parquet')


```

</td><td>

```python
  agg_df = df.groupby("District", maintain_order=True).agg([pl.mean("*"), pl.min("*")])
  
  apply_df = df.groupby("District", maintain_order=True).agg(
    pl.apply(  f=lambda spacePrice: spacePrice[0].sum() / spacePrice[1].sum(),
             exprs=["Space", "Price"] ) )
  
  df.to_parquet('polars1.parquet')
  loaded_df = pl.read_parquet('polars1.parquet')


```

</td></tr></table>

