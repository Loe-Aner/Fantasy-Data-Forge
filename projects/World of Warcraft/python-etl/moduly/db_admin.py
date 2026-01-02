from sqlalchemy import text

def czerwony_przycisk(
        silnik
    ):
    q = text("""
        DROP TABLE IF EXISTS dbo.MISJE_SLOWA_KLUCZOWE;
        DROP TABLE IF EXISTS dbo.MISJE_SLOWA;
        
        DROP TABLE IF EXISTS dbo.DIALOGI_STATUSY;
        DROP TABLE IF EXISTS dbo.MISJE_STATUSY;
        DROP TABLE IF EXISTS dbo.NPC_STATUSY;
        DROP TABLE IF EXISTS dbo.ZRODLO;
        
        DROP TABLE IF EXISTS dbo.MISJE;
        
        DROP TABLE IF EXISTS dbo.NPC;
        DROP TABLE IF EXISTS dbo.SLOWA_KLUCZOWE;
        DROP TABLE IF EXISTS dbo.LINKI_DO_SCRAPOWANIA;
""")
    
    with silnik.begin() as conn:
        conn.execute(q)
        print("Baza danych... już nie istnieje. Zadowolony jesteś z siebie?")