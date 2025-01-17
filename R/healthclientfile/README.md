1. create a view in MS SQL server with all the health client stacked together.
   1. fix the irregular DATE format. 
2. create a STUDY_ID dimension table
3. create a address dimension table with POSTAL_CODE, STREET_LINE, and other entity fields such as   [CITY], [STREET_LINE],           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
4. a function to clean the address in MS SQL server.
5. aggregate the view group by STUDY_ID and STREET_LINE and EFF_DATE to a history of STUDY_ID's movement aross different STREET_LINE and EFF_DATE. 