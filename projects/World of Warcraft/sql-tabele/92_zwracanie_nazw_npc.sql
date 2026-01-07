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
) -- fajna alternatywa dla unpivot

SELECT i.ID_NPC, ns.NAZWA
FROM idki AS i
INNER JOIN dbo.NPC_STATUSY AS ns
  ON i.ID_NPC = ns.NPC_ID_FK
WHERE ns.STATUS = '3_ZATWIERDZONO'
;