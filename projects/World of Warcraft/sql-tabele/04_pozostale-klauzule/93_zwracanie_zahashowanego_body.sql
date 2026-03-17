WITH hashe AS (
    SELECT m.MISJA_ID_MOJE_PK, z.HTML_SKOMPRESOWANY,
        ROW_NUMBER() OVER (PARTITION BY z.MISJA_ID_MOJE_FK ORDER BY z.DATA_WYSCRAPOWANIA DESC) AS r
    FROM dbo.ZRODLO AS z
    INNER JOIN dbo.MISJE AS m 
        ON z.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK
    WHERE 1=1 
        AND m.MISJA_ID_Z_GRY IS NOT NULL 
        AND m.MISJA_ID_Z_GRY <> 123456789
        AND (
            m.WSKAZNIK_ZGODNOSCI <= 0.70000
            OR m.WSKAZNIK_ZGODNOSCI IS NULL
        )
        
        --AND m.DODATEK_EN = 'Midnight'

        AND NOT EXISTS (
                        SELECT 1 
                        FROM dbo.MISJE_STATUSY AS ms 
                        WHERE ms.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK 
                            AND ms.STATUS = N'1_PRZETŁUMACZONO'
                        )
        AND EXISTS (
                        SELECT 1
                        FROM dbo.MISJE_STATUSY AS ms
                        WHERE 1=1
                            AND ms.MISJA_ID_MOJE_FK = m.MISJA_ID_MOJE_PK
                            AND ms.STATUS = N'0_ORYGINAŁ'
                    )
)
SELECT MISJA_ID_MOJE_PK, HTML_SKOMPRESOWANY 
FROM hashe 
WHERE r = 1 
;