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
