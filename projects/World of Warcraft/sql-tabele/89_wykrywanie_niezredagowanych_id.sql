WITH statusy_liczby AS (
    SELECT
        MISJA_ID_MOJE_FK,
        CAST(SUBSTRING(STATUS, 1, CHARINDEX('_', STATUS) - 1) AS INT) AS status_liczba
    FROM dbo.MISJE_STATUSY
    GROUP BY MISJA_ID_MOJE_FK, STATUS
),
misje_bez_redakcji AS (
    SELECT MISJA_ID_MOJE_FK
    FROM statusy_liczby
    GROUP BY MISJA_ID_MOJE_FK
    HAVING SUM(status_liczba) = 1
)

DELETE FROM dbo.MISJE_STATUSY
WHERE
    MISJA_ID_MOJE_FK IN (SELECT MISJA_ID_MOJE_FK FROM misje_bez_redakcji)
    AND STATUS = '1_PRZET£UMACZONO';