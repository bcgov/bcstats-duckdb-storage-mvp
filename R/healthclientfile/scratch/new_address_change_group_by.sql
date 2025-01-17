WITH COMBINED_HEALTH_TABLE AS  (
   SELECT '2020' AS effective_year, '02' AS effective_month, '13' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-01-01' AS [EFF_DATE],
             '2020-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200213_for_201107
 UNION ALL
    SELECT '2016' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-08-01' AS [EFF_DATE],
             '2016-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20160927
 UNION ALL
    SELECT '2016' AS effective_year, '10' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-09-01' AS [EFF_DATE],
             '2016-09-30' AS [END_DATE]
    FROM dev.CLR_EXT_20161027
 UNION ALL
    SELECT '2016' AS effective_year, '11' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-10-01' AS [EFF_DATE],
             '2016-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20161128
 UNION ALL
    SELECT '2016' AS effective_year, '12' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-11-01' AS [EFF_DATE],
             '2016-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20161229
 UNION ALL
    SELECT '2017' AS effective_year, '01' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2016-12-01' AS [EFF_DATE],
             '2016-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170126
 UNION ALL
    SELECT '2017' AS effective_year, '02' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-01-01' AS [EFF_DATE],
             '2017-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170227
 UNION ALL
    SELECT '2017' AS effective_year, '03' AS effective_month, '24' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-02-01' AS [EFF_DATE],
             '2017-02-28' AS [END_DATE]
    FROM dev.CLR_EXT_20170324
 UNION ALL
    SELECT '2017' AS effective_year, '04' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-03-01' AS [EFF_DATE],
             '2017-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170427
 UNION ALL
    SELECT '2017' AS effective_year, '06' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-05-01' AS [EFF_DATE],
             '2017-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170626
 UNION ALL
    SELECT '2017' AS effective_year, '07' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-06-01' AS [EFF_DATE],
             '2017-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20170727
 UNION ALL
    SELECT '2017' AS effective_year, '08' AS effective_month, '25' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-07-01' AS [EFF_DATE],
             '2017-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170825
 UNION ALL
    SELECT '2017' AS effective_year, '09' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-08-01' AS [EFF_DATE],
             '2017-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20170928
 UNION ALL
    SELECT '2017' AS effective_year, '10' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-09-01' AS [EFF_DATE],
             '2017-09-30' AS [END_DATE]
    FROM dev.CLR_EXT_20171026
 UNION ALL
    SELECT '2017' AS effective_year, '11' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-10-01' AS [EFF_DATE],
             '2017-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20171128
 UNION ALL
    SELECT '2017' AS effective_year, '12' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2017-11-01' AS [EFF_DATE],
             '2017-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20171228
 UNION ALL
    SELECT '2018' AS effective_year, '02' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-01-01' AS [EFF_DATE],
             '2018-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20180228
 UNION ALL
    SELECT '2018' AS effective_year, '03' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-02-01' AS [EFF_DATE],
             '2018-02-28' AS [END_DATE]
    FROM dev.CLR_EXT_20180327
 UNION ALL
    SELECT '2018' AS effective_year, '04' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-03-01' AS [EFF_DATE],
             '2018-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20180426
 UNION ALL
    SELECT '2018' AS effective_year, '05' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-04-01' AS [EFF_DATE],
             '2018-04-30' AS [END_DATE]
    FROM dev.CLR_EXT_20180529
 UNION ALL
    SELECT '2018' AS effective_year, '06' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-05-01' AS [EFF_DATE],
             '2018-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20180628
 UNION ALL
    SELECT '2018' AS effective_year, '08' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-07-01' AS [EFF_DATE],
             '2018-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20180829
 UNION ALL
    SELECT '2018' AS effective_year, '11' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-10-01' AS [EFF_DATE],
             '2018-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20181129
 UNION ALL
    SELECT '2018' AS effective_year, '12' AS effective_month, '24' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-11-01' AS [EFF_DATE],
             '2018-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20181224
 UNION ALL
    SELECT '2019' AS effective_year, '01' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-12-01' AS [EFF_DATE],
             '2018-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190128
 UNION ALL
    SELECT '2019' AS effective_year, '02' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-01-01' AS [EFF_DATE],
             '2019-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190228
 UNION ALL
    SELECT '2019' AS effective_year, '03' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-02-01' AS [EFF_DATE],
             '2019-02-28' AS [END_DATE]
    FROM dev.CLR_EXT_20190327
 UNION ALL
    SELECT '2019' AS effective_year, '04' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-03-01' AS [EFF_DATE],
             '2019-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190429
 UNION ALL
    SELECT '2019' AS effective_year, '06' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-05-01' AS [EFF_DATE],
             '2019-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190628
 UNION ALL
    SELECT '2019' AS effective_year, '07' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-06-01' AS [EFF_DATE],
             '2019-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20190729
 UNION ALL
    SELECT '2019' AS effective_year, '08' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-07-01' AS [EFF_DATE],
             '2019-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190829
 UNION ALL
    SELECT '2018' AS effective_year, '07' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2018-06-01' AS [EFF_DATE],
             '2018-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20180730
 UNION ALL
    SELECT '2019' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-08-01' AS [EFF_DATE],
             '2019-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20190927
 UNION ALL
    SELECT '2019' AS effective_year, '10' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-09-01' AS [EFF_DATE],
             '2019-09-30' AS [END_DATE]
    FROM dev.CLR_EXT_20191028
 UNION ALL
    SELECT '2019' AS effective_year, '11' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-10-01' AS [EFF_DATE],
             '2019-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20191128
 UNION ALL
    SELECT '2019' AS effective_year, '12' AS effective_month, '24' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-11-01' AS [EFF_DATE],
             '2019-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20191224
 UNION ALL
    SELECT '2020' AS effective_year, '01' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2019-12-01' AS [EFF_DATE],
             '2019-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200127
 UNION ALL
    SELECT '2020' AS effective_year, '02' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-01-01' AS [EFF_DATE],
             '2020-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200227
 UNION ALL
    SELECT '2020' AS effective_year, '03' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-02-01' AS [EFF_DATE],
             '2020-02-29' AS [END_DATE]
    FROM dev.CLR_EXT_20200330
 UNION ALL
    SELECT '2020' AS effective_year, '04' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-03-01' AS [EFF_DATE],
             '2020-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200427
 UNION ALL
    SELECT '2020' AS effective_year, '05' AS effective_month, '25' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-04-01' AS [EFF_DATE],
             '2020-04-30' AS [END_DATE]
    FROM dev.CLR_EXT_20200525
 UNION ALL
    SELECT '2020' AS effective_year, '06' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-05-01' AS [EFF_DATE],
             '2020-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200629
 UNION ALL
    SELECT '2020' AS effective_year, '07' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-06-01' AS [EFF_DATE],
             '2020-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20200729
 UNION ALL
    SELECT '2020' AS effective_year, '08' AS effective_month, '25' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-07-01' AS [EFF_DATE],
             '2020-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20200825
 UNION ALL
    SELECT '2020' AS effective_year, '11' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-10-01' AS [EFF_DATE],
             '2020-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20201130
 UNION ALL
    SELECT '2020' AS effective_year, '12' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-11-01' AS [EFF_DATE],
             '2020-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20201229
 UNION ALL
    SELECT '2021' AS effective_year, '01' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2020-12-01' AS [EFF_DATE],
             '2020-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210128
 UNION ALL
    SELECT '2021' AS effective_year, '02' AS effective_month, '25' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-01-01' AS [EFF_DATE],
             '2021-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210225
 UNION ALL
    SELECT '2021' AS effective_year, '04' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-03-01' AS [EFF_DATE],
             '2021-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210430
 UNION ALL
    SELECT '2021' AS effective_year, '06' AS effective_month, '09' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-05-01' AS [EFF_DATE],
             '2021-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210609
 UNION ALL
    SELECT '2021' AS effective_year, '06' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-05-01' AS [EFF_DATE],
             '2021-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210628
 UNION ALL
    SELECT '2021' AS effective_year, '07' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-06-01' AS [EFF_DATE],
             '2021-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20210729
 UNION ALL
    SELECT '2021' AS effective_year, '08' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-07-01' AS [EFF_DATE],
             '2021-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210830
 UNION ALL
    SELECT '2021' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-08-01' AS [EFF_DATE],
             '2021-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20210927
 UNION ALL
    SELECT '2021' AS effective_year, '10' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-09-01' AS [EFF_DATE],
             '2021-09-30' AS [END_DATE]
    FROM dev.CLR_EXT_20211028
 UNION ALL
    SELECT '2021' AS effective_year, '11' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-10-01' AS [EFF_DATE],
             '2021-10-31' AS [END_DATE]
    FROM dev.CLR_EXT_20211129
 UNION ALL
    SELECT '2021' AS effective_year, '12' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-11-01' AS [EFF_DATE],
             '2021-11-30' AS [END_DATE]
    FROM dev.CLR_EXT_20211230
 UNION ALL
    SELECT '2022' AS effective_year, '01' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-12-01' AS [EFF_DATE],
             '2021-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220128
 UNION ALL
    SELECT '2022' AS effective_year, '02' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-01-01' AS [EFF_DATE],
             '2022-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220228
 UNION ALL
    SELECT '2022' AS effective_year, '03' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-02-01' AS [EFF_DATE],
             '2022-02-28' AS [END_DATE]
    FROM dev.CLR_EXT_20220330
 UNION ALL
    SELECT '2022' AS effective_year, '04' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-03-01' AS [EFF_DATE],
             '2022-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220430
 UNION ALL
    SELECT '2022' AS effective_year, '05' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-04-01' AS [EFF_DATE],
             '2022-04-30' AS [END_DATE]
    FROM dev.CLR_EXT_20220530
 UNION ALL
    SELECT '2022' AS effective_year, '06' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-05-01' AS [EFF_DATE],
             '2022-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220627
 UNION ALL
    SELECT '2022' AS effective_year, '07' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-06-01' AS [EFF_DATE],
             '2022-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20220728
 UNION ALL
    SELECT '2022' AS effective_year, '08' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-07-01' AS [EFF_DATE],
             '2022-07-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220829
 UNION ALL
    SELECT '2022' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-08-01' AS [EFF_DATE],
             '2022-08-31' AS [END_DATE]
    FROM dev.CLR_EXT_20220927
 UNION ALL
    SELECT '2022' AS effective_year, '01' AS effective_month, '02' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-12-01' AS [EFF_DATE],
             '2021-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_202201028
 UNION ALL
    SELECT '2022' AS effective_year, '01' AS effective_month, '12' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2021-12-01' AS [EFF_DATE],
             '2021-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_202201129
 UNION ALL
    SELECT '2023' AS effective_year, '01' AS effective_month, '03' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2022-12-01' AS [EFF_DATE],
             '2022-12-31' AS [END_DATE]
    FROM dev.CLR_EXT_20230103
 UNION ALL
    SELECT '2023' AS effective_year, '02' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-01-01' AS [EFF_DATE],
             '2023-01-31' AS [END_DATE]
    FROM dev.CLR_EXT_20230227
 UNION ALL
    SELECT '2023' AS effective_year, '03' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-02-01' AS [EFF_DATE],
             '2023-02-28' AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20230327
 UNION ALL
    SELECT '2023' AS effective_year, '04' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-03-01' AS [EFF_DATE],
             '2023-03-31' AS [END_DATE]
    FROM dev.CLR_EXT_20230426
 UNION ALL
    SELECT '2023' AS effective_year, '05' AS effective_month, '25' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-04-01' AS [EFF_DATE],
             '2023-04-30' AS [END_DATE]
    FROM dev.CLR_EXT_20230525
 UNION ALL
    SELECT '2023' AS effective_year, '06' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-05-01' AS [EFF_DATE],
             '2023-05-31' AS [END_DATE]
    FROM dev.CLR_EXT_20230626
 UNION ALL
    SELECT '2023' AS effective_year, '07' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '2023-06-01' AS [EFF_DATE],
             '2023-06-30' AS [END_DATE]
    FROM dev.CLR_EXT_20230726
 UNION ALL
    SELECT '2023' AS effective_year, '08' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-07-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-07-31') AS [END_DATE]
    FROM dev.CLR_EXT_20230828
 UNION ALL
    SELECT '2023' AS effective_year, '09' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-08-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-08-31') AS [END_DATE]
    FROM dev.CLR_EXT_20230927
 UNION ALL
    SELECT '2023' AS effective_year, '10' AS effective_month, '30' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-09-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-09-30') AS [END_DATE]
    FROM dev.CLR_EXT_20231030
 UNION ALL
    SELECT '2023' AS effective_year, '11' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-10-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-10-31') AS [END_DATE]
    FROM dev.CLR_EXT_20231127
 UNION ALL
    SELECT '2023' AS effective_year, '12' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-11-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-11-30') AS [END_DATE]
    FROM dev.CLR_EXT_20231227
 UNION ALL
    SELECT '2024' AS effective_year, '01' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2023-12-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2023-12-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240129
 UNION ALL
    SELECT '2024' AS effective_year, '02' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-01-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-01-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240226
 UNION ALL
    SELECT '2024' AS effective_year, '03' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-02-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-02-29') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240326
 UNION ALL
    SELECT '2024' AS effective_year, '04' AS effective_month, '29' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-03-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-03-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240429
 UNION ALL
    SELECT '2024' AS effective_year, '05' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-04-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-04-30') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240527
 UNION ALL
    SELECT '2024' AS effective_year, '06' AS effective_month, '28' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-05-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-05-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240628
 UNION ALL
    SELECT '2024' AS effective_year, '07' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-06-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-06-30') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240726
 UNION ALL
    SELECT '2024' AS effective_year, '08' AS effective_month, '27' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-07-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-07-31') AS [END_DATE]
    FROM dev.BC_STAT_POPULATION_ESTIMATES_20240827
 UNION ALL
    SELECT '2024' AS effective_year, '09' AS effective_month, '26' AS effective_day,
           [STUDY_ID],  [POSTAL_CODE], [CITY], [STREET_LINE],
           [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '2024-08-01') AS [EFF_DATE],
             ISNULL([END_DATE], '2024-08-31') AS [END_DATE]
    FROM dev.bc_stat_population_estimates_20240926
),
CleanedData AS (
    SELECT
        STUDY_ID,
        POSTAL_CODE,
        -- STREET_LINE,
        dbo.clean_street_line(STREET_LINE, CITY) AS STREET_LINE,
        LEFT(STREET_LINE, CHARINDEX(' ', dbo.clean_street_line(STREET_LINE, CITY) + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1) AS STREET_FIRST_TWO_WORDS,
        EFF_DATE,
        END_DATE,
        FORMAT(TRY_CONVERT(DATE, EFF_DATE), 'yyyy-MM') AS EFF_MONTH, -- Convert and extract EFF_MONTH
        FORMAT(TRY_CONVERT(DATE, END_DATE), 'yyyy-MM') AS END_MONTH -- Convert and extract END_MONTH
        -- Calculate lagged END_DATE for comparison
        -- LAG(END_DATE) OVER (
        --     PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE
        --     ORDER BY EFF_DATE
        -- ) AS Prev_END_DATE,
        -- LAG(FORMAT(TRY_CONVERT(DATE, END_DATE), 'yyyy-MM')) OVER (
        --     PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE
        --     ORDER BY EFF_DATE
        -- ) AS Prev_END_MONTH,
        -- LEAD(FORMAT(TRY_CONVERT(DATE, EFF_DATE), 'yyyy-MM')) OVER (
        --     PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE
        --     ORDER BY EFF_DATE
        -- ) AS Next_EFF_MONTH,
        -- -- Convert EFF_MONTH and Prev_END_MONTH to dates
        -- CAST(FORMAT(TRY_CONVERT(DATE, EFF_DATE), 'yyyy-MM') + '-01' AS DATE) AS EFF_MONTH_DATE,
        -- CAST(LAG(FORMAT(TRY_CONVERT(DATE, END_DATE), 'yyyy-MM')) OVER (
        --     PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE
        --     ORDER BY EFF_DATE
        -- ) + '-01' AS DATE) AS Prev_END_MONTH_DATE
        
    FROM COMBINED_HEALTH_TABLE
),
 StreetFirstTwoWords AS (
    SELECT
        STUDY_ID,
        POSTAL_CODE,
        STREET_LINE,
        STREET_FIRST_TWO_WORDS,
        EFF_MONTH,
        END_MONTH,
        LAG(POSTAL_CODE) OVER (
                PARTITION BY STUDY_ID ORDER BY EFF_MONTH
            ) as prev_postal_code,
        LAG(STREET_FIRST_TWO_WORDS) OVER (
                PARTITION BY STUDY_ID ORDER BY EFF_MONTH
            ) as prev_STREET_FIRST_TWO_WORDS
    FROM CleanedData
),
GroupKeyMonthData AS (
    SELECT
        STUDY_ID,
        POSTAL_CODE,
        STREET_FIRST_TWO_WORDS,
        EFF_MONTH,
        END_MONTH,
        -- Create GroupKeyMonth based on changes in address or postal code
        SUM(CASE
            WHEN (prev_postal_code  IS NULL) 
            or (prev_postal_code != POSTAL_CODE) 
            or  (prev_STREET_FIRST_TWO_WORDS IS NULL ) 
            OR (prev_STREET_FIRST_TWO_WORDS != STREET_FIRST_TWO_WORDS)
            THEN 1 ELSE 0
        END) OVER (PARTITION BY STUDY_ID ORDER BY EFF_MONTH) AS GroupKeyMonth
    FROM StreetFirstTwoWords
),
AggregatedPeriods AS (
    SELECT
        STUDY_ID,
        POSTAL_CODE,
        STREET_FIRST_TWO_WORDS,
        MIN(EFF_MONTH) AS StartMonth, -- Minimum effective month
        MAX(END_MONTH) AS EndMonth -- Maximum end month
    FROM GroupKeyMonthData
    GROUP BY STUDY_ID, POSTAL_CODE, STREET_FIRST_TWO_WORDS, GroupKeyMonth

)
SELECT *
   --  STUDY_ID,
   --  POSTAL_CODE,
   --  STREET_LINE,
   --  StartMonth AS EFF_MONTH,
   --  EndMonth AS END_MONTH
   --  INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_ORDERED_MONTH
FROM  AggregatedPeriods
WHERE STUDY_ID = '001A9042FA9A48CC00E28DD031448F25B9CE08AA676282D6'
ORDER BY STUDY_ID, StartMonth;

