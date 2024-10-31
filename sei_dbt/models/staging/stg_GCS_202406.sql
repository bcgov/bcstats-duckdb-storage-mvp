-- Load raw sales data from the CSV file
SELECT * FROM
read_csv_auto('{{ target.external_root }}/TMF/GCS_202406.csv')
