-- Read data from a CSV file within a zip archive
SELECT * FROM
read_csv_auto('{{ target.external_root }}/bca_folio/bca_folio_gnrl_property_values_20240825_WEEKLY.csv')
