-- Load raw sales data from the CSV file
SELECT * FROM
read_csv_auto('{{ target.external_root }}/Census 2021/Census 2021 PUMF data_donnees_2021_ind_v2.csv')
