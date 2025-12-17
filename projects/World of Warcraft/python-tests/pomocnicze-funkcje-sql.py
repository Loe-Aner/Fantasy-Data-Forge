from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

# zastanowic sie nad WHERE i innymi
def pobierz_dane(
        silnik, 
        tabela: str, 
        kolumny_FROM: list | None = None,
        top: int | None = None
        ):
    t = f"TOP {top}" if top is not None else ""
    k = ", ".join(kolumny_FROM) if kolumny_FROM is not None else "*"

    q = text(f"""
      SELECT {t}
             {k}
      FROM {tabela}
    """)
    
    with silnik.connect() as conn:
        wiersze = conn.execute(q).mappings().all()
        return [dict(w) for w in wiersze]
    

def czerwony_przycisk(
        silnik
    ):
    q = text("""
        DROP TABLE IF EXISTS dbo.NPC_STATUSY;
        DROP TABLE IF EXISTS dbo.MISJE_STATUSY
        DROP TABLE IF EXISTS dbo.ZRODLO;
        DROP TABLE IF EXISTS dbo.MISJE;
        DROP TABLE IF EXISTS dbo.NPC;
""")
    
    with silnik.begin() as conn:
        conn.execute(q)

def dodaj_npc_do_db(silnik, nazwa: str) -> str:
    q = text("""
        INSERT INTO dbo.NPC (NAZWA)
        VALUES (:nazwa);
    """)

    try:
        with silnik.begin() as conn:
            conn.execute(q, {"nazwa": nazwa})
        return "Dodano NPCa do bazy danych."
    except IntegrityError as e:
        if e.orig and any("2627" in str(arg) or "2601" in str(arg) for arg in e.orig.args):
            return "NPC już istnieje w bazie danych."
        raise