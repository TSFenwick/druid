SELECT col5 , LAG(col5) OVER ( PARTITION BY col3 ORDER BY col1 ) LAG_col5 FROM "fewRowsAllData.parquet"