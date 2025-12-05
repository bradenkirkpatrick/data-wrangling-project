DROP TABLE IF EXISTS Batting;
-- ONLY select the relevant columns for batting statistics (currently includes all columns)
CREATE TABLE Batting AS
SELECT
    Rk,
    Player▲ as name,
    Age,
    Team,
    Lg,
    G,
    PA,
    AB,
    R,
    H,
    "2B",
    "3B",
    HR,
    RBI,
    SB,
    CS,
    BB,
    SO,
    HBP,
    SH,
    SF,
    IBB,
    -- New 'Year' column using filename extraction
    regexp_extract(filename, '20[0-9]{2}')::INTEGER AS Year
FROM read_csv_auto('data/20* MLB Batting Stats.csv')

UNION ALL

SELECT
    Rk,
    Name,
    Age,
    Tm as Team,
    Lg,
    G,
    PA,
    AB,
    R,
    H,
    "2B",
    "3B",
    HR,
    RBI,
    SB,
    CS,
    BB,
    SO,
    HBP,
    SH,
    SF,
    IBB,
    -- New 'Year' column using filename extraction
    regexp_extract(filename, '20[0-9]{2}')::INTEGER AS Year
FROM read_csv_auto('data/2022 MLB Player Stats - Batting.csv', encoding='latin-1', delim=';');