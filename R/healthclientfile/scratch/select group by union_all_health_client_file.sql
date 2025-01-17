CREATE FUNCTION dbo.clean_street_line (
    @street_line NVARCHAR(MAX),
    @city NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @cleaned NVARCHAR(MAX);

    -- Convert to uppercase
    SET @cleaned = UPPER(@street_line);

    -- Remove extra whitespace
    SET @cleaned = LTRIM(RTRIM(@cleaned));
    SET @cleaned = REPLACE(@cleaned, '  ', ' '); -- Repeatedly remove double spaces
    WHILE CHARINDEX('  ', @cleaned) > 0
        SET @cleaned = REPLACE(@cleaned, '  ', ' ');

    -- Remove non-alphanumeric characters
    SET @cleaned = REPLACE(@cleaned, ',', '');
    SET @cleaned = REPLACE(@cleaned, '.', '');
    SET @cleaned = REPLACE(@cleaned, '/', '');
    SET @cleaned = REPLACE(@cleaned, '-', '');
    -- Add more REPLACE() calls as needed for other non-alphanumeric characters

    -- Remove "BC" abbreviation if redundant
    SET @cleaned = REPLACE(@cleaned, ' BC ', ' ');

    -- Remove the city name
    SET @cleaned = REPLACE(@cleaned, ' ' + UPPER(@city) + ' ', ' ');

    -- Remove extra whitespace again after all replacements
    SET @cleaned = LTRIM(RTRIM(@cleaned));
    WHILE CHARINDEX('  ', @cleaned) > 0
        SET @cleaned = REPLACE(@cleaned, '  ', ' ');

    RETURN @cleaned;
END;







DROP TABLE DEV.FCT_HEALTH_CLIENT_CLEAN;
WITH COMBINED_HEALTH_TABLE AS  (
   SELECT TOP (1000) '2020' AS effective_year, '02' AS effective_month, '13' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-01-01' AS [EFF_DATE],
             '2020-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200213_for_201107
 UNION ALL
    SELECT TOP (1000) '2016' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-08-01' AS [EFF_DATE],
             '2016-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20160927
 UNION ALL
    SELECT TOP (1000) '2016' AS effective_year, '10' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-09-01' AS [EFF_DATE],
             '2016-09-30' AS [END_DATE]
    FROM dev.CLR_EXT_20161027
 UNION ALL
    SELECT TOP (1000) '2016' AS effective_year, '11' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-10-01' AS [EFF_DATE],
             '2016-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20161128
 UNION ALL
    SELECT TOP (1000) '2016' AS effective_year, '12' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-11-01' AS [EFF_DATE],
             '2016-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20161229
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '01' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-12-01' AS [EFF_DATE],
             '2016-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170126
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '02' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-01-01' AS [EFF_DATE],
             '2017-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170227
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '03' AS effective_month, '24' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-02-01' AS [EFF_DATE],
             '2017-02-28' AS [END_DATE]
    FROM dev.CLR_EXT_20170324
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '04' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-03-01' AS [EFF_DATE],
             '2017-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170427
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '06' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-05-01' AS [EFF_DATE],
             '2017-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170626
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '07' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-06-01' AS [EFF_DATE],
             '2017-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20170727
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '08' AS effective_month, '25' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-07-01' AS [EFF_DATE],
             '2017-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170825
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '09' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-08-01' AS [EFF_DATE],
             '2017-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170928
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '10' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-09-01' AS [EFF_DATE],
             '2017-09-30' AS [END_DATE]
    FROM dev.CLR_EXT_20171026
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '11' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-10-01' AS [EFF_DATE],
             '2017-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20171128
 UNION ALL
    SELECT TOP (1000) '2017' AS effective_year, '12' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-11-01' AS [EFF_DATE],
             '2017-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20171228
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '02' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-01-01' AS [EFF_DATE],
             '2018-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20180228
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '03' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-02-01' AS [EFF_DATE],
             '2018-02-28' AS [END_DATE]
    FROM dev.CLR_EXT_20180327
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '04' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-03-01' AS [EFF_DATE],
             '2018-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20180426
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '05' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-04-01' AS [EFF_DATE],
             '2018-04-30' AS [END_DATE]
    FROM dev.CLR_EXT_20180529
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '06' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-05-01' AS [EFF_DATE],
             '2018-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20180628
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '08' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-07-01' AS [EFF_DATE],
             '2018-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20180829
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '11' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-10-01' AS [EFF_DATE],
             '2018-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20181129
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '12' AS effective_month, '24' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-11-01' AS [EFF_DATE],
             '2018-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20181224
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '01' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-12-01' AS [EFF_DATE],
             '2018-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190128
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '02' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-01-01' AS [EFF_DATE],
             '2019-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190228
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '03' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-02-01' AS [EFF_DATE],
             '2019-02-28' AS [END_DATE]
    FROM dev.CLR_EXT_20190327
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '04' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-03-01' AS [EFF_DATE],
             '2019-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190429
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '06' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-05-01' AS [EFF_DATE],
             '2019-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190628
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '07' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-06-01' AS [EFF_DATE],
             '2019-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20190729
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '08' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-07-01' AS [EFF_DATE],
             '2019-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190829
 UNION ALL
    SELECT TOP (1000) '2018' AS effective_year, '07' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-06-01' AS [EFF_DATE],
             '2018-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20180730
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-08-01' AS [EFF_DATE],
             '2019-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190927
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '10' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-09-01' AS [EFF_DATE],
             '2019-09-30' AS [END_DATE]
    FROM dev.CLR_EXT_20191028
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '11' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-10-01' AS [EFF_DATE],
             '2019-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20191128
 UNION ALL
    SELECT TOP (1000) '2019' AS effective_year, '12' AS effective_month, '24' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-11-01' AS [EFF_DATE],
             '2019-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20191224
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '01' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-12-01' AS [EFF_DATE],
             '2019-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200127
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '02' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-01-01' AS [EFF_DATE],
             '2020-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200227
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '03' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-02-01' AS [EFF_DATE],
             '2020-02-29' AS [END_DATE]
    FROM dev.CLR_EXT_20200330
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '04' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-03-01' AS [EFF_DATE],
             '2020-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200427
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '05' AS effective_month, '25' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-04-01' AS [EFF_DATE],
             '2020-04-30' AS [END_DATE]
    FROM dev.CLR_EXT_20200525
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '06' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-05-01' AS [EFF_DATE],
             '2020-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200629
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '07' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-06-01' AS [EFF_DATE],
             '2020-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20200729
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '08' AS effective_month, '25' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-07-01' AS [EFF_DATE],
             '2020-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200825
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '11' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-10-01' AS [EFF_DATE],
             '2020-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20201130
 UNION ALL
    SELECT TOP (1000) '2020' AS effective_year, '12' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-11-01' AS [EFF_DATE],
             '2020-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20201229
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '01' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-12-01' AS [EFF_DATE],
             '2020-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210128
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '02' AS effective_month, '25' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-01-01' AS [EFF_DATE],
             '2021-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210225
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '04' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-03-01' AS [EFF_DATE],
             '2021-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210430
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '06' AS effective_month, '09' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-05-01' AS [EFF_DATE],
             '2021-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210609
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '06' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-05-01' AS [EFF_DATE],
             '2021-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210628
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '07' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-06-01' AS [EFF_DATE],
             '2021-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20210729
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '08' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-07-01' AS [EFF_DATE],
             '2021-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210830
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-08-01' AS [EFF_DATE],
             '2021-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210927
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '10' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-09-01' AS [EFF_DATE],
             '2021-09-30' AS [END_DATE]
    FROM dev.CLR_EXT_20211028
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '11' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-10-01' AS [EFF_DATE],
             '2021-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20211129
 UNION ALL
    SELECT TOP (1000) '2021' AS effective_year, '12' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-11-01' AS [EFF_DATE],
             '2021-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20211230
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '01' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-12-01' AS [EFF_DATE],
             '2021-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220128
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '02' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-01-01' AS [EFF_DATE],
             '2022-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220228
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '03' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-02-01' AS [EFF_DATE],
             '2022-02-28' AS [END_DATE]
    FROM dev.CLR_EXT_20220330
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '04' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-03-01' AS [EFF_DATE],
             '2022-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220430
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '05' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-04-01' AS [EFF_DATE],
             '2022-04-30' AS [END_DATE]
    FROM dev.CLR_EXT_20220530
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '06' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-05-01' AS [EFF_DATE],
             '2022-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220627
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '07' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-06-01' AS [EFF_DATE],
             '2022-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20220728
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '08' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-07-01' AS [EFF_DATE],
             '2022-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220829
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-08-01' AS [EFF_DATE],
             '2022-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220927
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '01' AS effective_month, '02' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-12-01' AS [EFF_DATE],
             '2021-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_202201028
 UNION ALL
    SELECT TOP (1000) '2022' AS effective_year, '01' AS effective_month, '12' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-12-01' AS [EFF_DATE],
             '2021-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_202201129
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '01' AS effective_month, '03' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-12-01' AS [EFF_DATE],
             '2022-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20230103
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '02' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-01-01' AS [EFF_DATE],
             '2023-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20230227
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '03' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-02-01' AS [EFF_DATE],
             '2023-02-28' AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20230327
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '04' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-03-01' AS [EFF_DATE],
             '2023-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20230426
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '05' AS effective_month, '25' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-04-01' AS [EFF_DATE],
             '2023-04-30' AS [END_DATE]
    FROM dev.CLR_EXT_20230525
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '06' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-05-01' AS [EFF_DATE],
             '2023-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20230626
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '07' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-06-01' AS [EFF_DATE],
             '2023-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20230726
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '08' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-07-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-07-31') AS [END_DATE]
    FROM dev.CLR_EXT_20230828
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-08-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-08-31') AS [END_DATE]
    FROM dev.CLR_EXT_20230927
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '10' AS effective_month, '30' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-09-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-09-30') AS [END_DATE]
    FROM dev.CLR_EXT_20231030
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '11' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-10-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-10-31') AS [END_DATE]
    FROM dev.CLR_EXT_20231127
 UNION ALL
    SELECT TOP (1000) '2023' AS effective_year, '12' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-11-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-11-30') AS [END_DATE]
    FROM dev.CLR_EXT_20231227
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '01' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-12-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-12-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240129
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '02' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-01-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-01-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240226
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '03' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-02-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-02-29') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240326
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '04' AS effective_month, '29' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-03-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-03-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240429
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '05' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-04-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-04-30') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240527
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '06' AS effective_month, '28' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-05-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-05-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240628
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '07' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-06-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-06-30') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240726
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '08' AS effective_month, '27' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-07-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-07-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240827
 UNION ALL
    SELECT TOP (1000) '2024' AS effective_year, '09' AS effective_month, '26' AS effective_day,
           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-08-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-08-31') AS [END_DATE]
    FROM dev.bc_stat_population_estimates_20240926
),

CleanedData AS (
    SELECT
        STUDY_ID,
        BIRTH_YR_MON,
        SEX,
        POSTAL_CODE,
        CITY,
        STREET_LINE,
        dbo.clean_street_line(STREET_LINE, CITY) AS STREET_LINE_CLEAN,
        EFF_DATE,
        END_DATE
    FROM COMBINED_HEALTH_TABLE
),
CanonicalAddress AS (
    SELECT
        STUDY_ID,
        BIRTH_YR_MON,
        SEX,
        POSTAL_CODE,
        CITY,
        STREET_LINE_CLEAN,
        ROW_NUMBER() OVER (
            PARTITION BY STUDY_ID, BIRTH_YR_MON, SEX, POSTAL_CODE
            ORDER BY LEN(STREET_LINE_CLEAN) DESC
        ) AS RowNum
    FROM CleanedData
),
CanonicalData AS (
    SELECT
        c.STUDY_ID,
        c.BIRTH_YR_MON,
        c.SEX,
        c.POSTAL_CODE,
        c.STREET_LINE_CLEAN,
        MIN(d.EFF_DATE) AS EFF_DATE,
        MAX(d.END_DATE) AS END_DATE
    FROM CanonicalAddress c
    INNER JOIN CleanedData d
        ON c.STUDY_ID = d.STUDY_ID
        AND c.BIRTH_YR_MON = d.BIRTH_YR_MON
        AND c.SEX = d.SEX
        AND c.POSTAL_CODE = d.POSTAL_CODE
        AND c.STREET_LINE_CLEAN = d.STREET_LINE_CLEAN
    WHERE c.RowNum = 1
    GROUP BY c.STUDY_ID, c.BIRTH_YR_MON, c.SEX, c.POSTAL_CODE, c.STREET_LINE_CLEAN
)
SELECT
    STUDY_ID,
    BIRTH_YR_MON,
    SEX,
    POSTAL_CODE,
    STREET_LINE_CLEAN AS STREET_LINE,
    EFF_DATE,
    END_DATE
    INTO dev.FCT_HEALTH_CLIENT_CLEAN
FROM CanonicalData
    ORDER BY STUDY_ID,
    BIRTH_YR_MON,
    SEX,
    POSTAL_CODE,
    STREET_LINE_CLEAN;






