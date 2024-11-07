-- Read data from a CSV file  -- , auto_type_candidates = ['VARCHAR']
SELECT * FROM
read_csv_auto('{{ target.external_root }}/bca_folio/bca_folio_addresses_20240825_WEEKLY.csv', all_varchar = true, ignore_errors=true )
