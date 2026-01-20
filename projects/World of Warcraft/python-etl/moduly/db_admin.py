from sqlalchemy import text

LISTA_PLIKOW = [
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\1_tabela_NPC.sql",
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\2_tabela_misje.sql",
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\3_tabela_zrodlo.sql",
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\4_tabela_NPC_statusy.sql",
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\5_tabela_misje_statusy.sql",
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\6_tabela_dialogi.sql",
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\7_tabela_linki_do_scrapowania.sql",
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\8_tabela_slowa_kluczowe.sql",
    r"D:\MyProjects_4Fun\projects\World of Warcraft\sql-tabele\9_tabela_misje_slowa_kluczowe.sql"
]

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

def zielony_przycisk(silnik, lista = LISTA_PLIKOW):
    with silnik.connect() as conn:
        for sciezka in lista:
            with open(sciezka, "r", encoding="cp1250") as f:
                tresc = f.read()
            komendy = tresc.split(";")
            for k in komendy:
                if k.strip():
                    conn.execute(text(k))
            
            conn.commit()
            print(f"Postawiono tabelę w: {sciezka}")