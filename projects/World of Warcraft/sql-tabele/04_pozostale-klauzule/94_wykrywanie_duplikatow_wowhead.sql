WITH a AS (
SELECT 
	MISJA_ID_Z_GRY
FROM dbo.MISJE
GROUP BY
	MISJA_ID_Z_GRY
HAVING COUNT(MISJA_ID_Z_GRY) > 1
)

SELECT
	b.MISJA_ID_MOJE_PK,
	b.MISJA_ID_Z_GRY,
	b.MISJA_URL_WIKI,
	b.MISJA_URL_WOWHEAD,
	b.MISJA_TYTUL_EN
FROM dbo.MISJE AS b
WHERE 1=1
  AND EXISTS (SELECT 1 FROM a WHERE a.MISJA_ID_Z_GRY = b.MISJA_ID_Z_GRY)
  AND MISJA_ID_Z_GRY != 123456789; -- to id nigdy nie bedzie brane pod uwage
								   -- misje na wow.wiki maja czasami 3 linki:
								   -- Pierwszy: ogolna misja, Druga: Horda, Trzecia: Alliance
								   -- ta ogolna misja to wlasnie 123456789 ktora w grze nie wystepuje
;

