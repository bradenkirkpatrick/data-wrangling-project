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
    2B,
    3B,
    HR,
    RBI,
    SB,
    CS,
    BB,
    SO,
    HBP,
    SH,
    SF,
    IBB
FROM read_csv_auto('data/20* MLB Batting Stats.csv')

UNION

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
    2B,
    3B,
    HR,
    RBI,
    SB,
    CS,
    BB,
    SO,
    HBP,
    SH,
    SF,
    IBB
FROM read_csv_auto('data/2022 MLB Batting Stats(has different columns).csv', encoding='latin-1', delim=';');

-- this makes the table Batting with consistent column names:
-- duckdb baseball_data.db < baseball_data.sql 