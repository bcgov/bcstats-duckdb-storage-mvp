-- Load raw sales data from the CSV file
SELECT * FROM
read_csv_auto('{{ target.external_root }}/DIM_CMA_CD.csv')
