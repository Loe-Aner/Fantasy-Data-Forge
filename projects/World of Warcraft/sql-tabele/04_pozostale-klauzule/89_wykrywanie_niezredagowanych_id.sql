DROP TABLE IF EXISTS #misje_bez_redakcji;

WITH statusy_liczby AS (
    SELECT
        MISJA_ID_MOJE_FK,
        CAST(SUBSTRING(STATUS, 1, CHARINDEX('_', STATUS) - 1) AS INT) AS status_liczba
    FROM dbo.MISJE_STATUSY
    GROUP BY MISJA_ID_MOJE_FK, STATUS
)
SELECT MISJA_ID_MOJE_FK
INTO #misje_bez_redakcji
FROM statusy_liczby
GROUP BY MISJA_ID_MOJE_FK
HAVING SUM(status_liczba) = 1;

DELETE cel
FROM dbo.MISJE_STATUSY cel
INNER JOIN #misje_bez_redakcji zrodlo 
  ON cel.MISJA_ID_MOJE_FK = zrodlo.MISJA_ID_MOJE_FK
WHERE cel.STATUS = '1_PRZET£UMACZONO';

DELETE cel
FROM dbo.DIALOGI_STATUSY cel
INNER JOIN #misje_bez_redakcji zrodlo 
  ON cel.MISJA_ID_MOJE_FK = zrodlo.MISJA_ID_MOJE_FK
WHERE cel.STATUS = '1_PRZET£UMACZONO';

DROP TABLE #misje_bez_redakcji;