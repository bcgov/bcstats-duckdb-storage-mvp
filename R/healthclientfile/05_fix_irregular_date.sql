SELECT EFF_DATE, END_DATE
FROM [HealthFiles_test].[dev].[FCT_HEALTH_CLIENT_ADDRESS_ORDERED]
WHERE EFF_DATE IS NOT NULL AND END_DATE IS NOT NULL
  AND (EFF_DATE LIKE '%/%' OR END_DATE LIKE '%/%');


---//--
--TRY_CONVERT:

--Attempts to convert the input string to a DATE type.
--The 111 style specifies the YYYY/MM/DD format, which is commonly used in irregular date formats.
--If the conversion fails, TRY_CONVERT returns NULL without throwing an error.
--CONVERT:
--
--Converts the date to a standardized YYYY-MM-DD format using the 120 style.
--WHERE Clause:
--
--Ensures the update applies only to rows where the irregular date is valid and can be converted.
--//
-- Update irregularly formatted EFF_DATE and END_DATE
UPDATE [HealthFiles_test].[dev].[FCT_HEALTH_CLIENT_ADDRESS_DATE]
SET EFF_DATE = CONVERT(VARCHAR(10), TRY_CONVERT(DATE, EFF_DATE, 111), 120),
    END_DATE = CONVERT(VARCHAR(10), TRY_CONVERT(DATE, END_DATE, 111), 120)
WHERE TRY_CONVERT(DATE, EFF_DATE, 111) IS NOT NULL
   OR TRY_CONVERT(DATE, END_DATE, 111) IS NOT NULL;

-- If the irregular format contains other separators (e.g., dots or spaces), you can preprocess the string with REPLACE to make it consistent before conversion.

UPDATE [HealthFiles_test].[dev].[FCT_HEALTH_CLIENT_ADDRESS_DATE]
SET EFF_DATE = CONVERT(VARCHAR(10), TRY_CONVERT(DATE, REPLACE(EFF_DATE, '/', '-'), 120), 120),
    END_DATE = CONVERT(VARCHAR(10), TRY_CONVERT(DATE, REPLACE(END_DATE, '/', '-'), 120), 120)
WHERE TRY_CONVERT(DATE, REPLACE(EFF_DATE, '/', '-'), 120) IS NOT NULL
   OR TRY_CONVERT(DATE, REPLACE(END_DATE, '/', '-'), 120) IS NOT NULL;
