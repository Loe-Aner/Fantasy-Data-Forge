DROP TABLE IF EXISTS #misje_z_redakcja;

WITH statusy_liczby AS (
    SELECT
        MISJA_ID_MOJE_FK,
        CAST(SUBSTRING(STATUS, 1, CHARINDEX('_', STATUS) - 1) AS INT) AS status_liczba
    FROM dbo.MISJE_STATUSY
    GROUP BY MISJA_ID_MOJE_FK, STATUS
)
SELECT MISJA_ID_MOJE_FK
INTO #misje_z_redakcja
FROM statusy_liczby
GROUP BY MISJA_ID_MOJE_FK
HAVING SUM(status_liczba) = 6
;

SELECT *
FROM #misje_z_redakcja
