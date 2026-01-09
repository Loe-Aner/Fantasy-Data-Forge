WITH idki AS (
    SELECT DISTINCT
        tabela_wartosci.ID_NPC
    FROM dbo.MISJE AS m
    CROSS APPLY (
        VALUES
            (m.NPC_START_ID),
            (m.NPC_KONIEC_ID)
    ) AS tabela_wartosci (ID_NPC)
    WHERE m.MISJA_ID_MOJE_PK = 3
),

oczyszczone_dane AS (
    SELECT 
        i.ID_NPC,
        ns.STATUS,
        CASE 
            WHEN CHARINDEX('[', ns.NAZWA) > 0 
            THEN RTRIM(LEFT(ns.NAZWA, CHARINDEX('[', ns.NAZWA) - 1))
            ELSE ns.NAZWA 
        END AS CZYSTA_NAZWA
    FROM idki AS i
    INNER JOIN dbo.NPC_STATUSY AS ns
        ON i.ID_NPC = ns.NPC_ID_FK
)

SELECT 
    pvt.[0_ORYGINA£], 
    pvt.[3_ZATWIERDZONO]
FROM oczyszczone_dane
PIVOT (
    MAX(CZYSTA_NAZWA)
    FOR STATUS IN ([0_ORYGINA£], [3_ZATWIERDZONO])
) AS pvt