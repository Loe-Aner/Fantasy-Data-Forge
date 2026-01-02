from sqlalchemy import text

def zapisz_zrodlo_do_db(
        silnik,
        tabela_zrodlo: str,
        misja_id: int,
        wynik: dict,
        zrodlo: str
    ) -> int:

    hash_html = wynik.get("Hash_HTML", {}) or {}

    hash_glowny_cel = (hash_html.get("Cele_EN", {}) or {}).get("Główny")
    hash_podrzedny_cel = (hash_html.get("Cele_EN", {}) or {}).get("Podrzędny")

    hash_tresc = hash_html.get("Treść_EN")
    hash_postep = hash_html.get("Postęp_EN")
    hash_zakonczenie = hash_html.get("Zakończenie_EN")
    hash_nagrody = hash_html.get("Nagrody_EN")

    hash_dymki = (hash_html.get("Dialogi_EN", {}) or {}).get("Dymki_EN")
    hash_gossip = (hash_html.get("Dialogi_EN", {}) or {}).get("Gossipy_EN")

    q_insert = text(f"""
        INSERT INTO {tabela_zrodlo} (
            MISJA_ID_MOJE_FK, ZRODLO_NAZWA,
            HTML_HASH_GLOWNY_CEL, HTML_HASH_PODRZEDNY_CEL,
            HTML_HASH_TRESC, HTML_HASH_POSTEP, HTML_HASH_ZAKONCZENIE, HTML_HASH_NAGRODY,
            HTML_HASH_DYMKI, HTML_HASH_GOSSIP
        )
        OUTPUT inserted.TECH_ID
        VALUES (
            :misja_id_fk, :zrodlo_nazwa,
            :hash_glowny_cel, :hash_podrzedny_cel,
            :hash_tresc, :hash_postep, :hash_zakonczenie, :hash_nagrody,
            :hash_dymki, :hash_gossip
        );
    """)

    with silnik.begin() as conn:
        tech_id = conn.execute(
            q_insert,
            {
                "misja_id_fk": misja_id,
                "zrodlo_nazwa": zrodlo,
                "hash_glowny_cel": hash_glowny_cel,
                "hash_podrzedny_cel": hash_podrzedny_cel,
                "hash_tresc": hash_tresc,
                "hash_postep": hash_postep,
                "hash_zakonczenie": hash_zakonczenie,
                "hash_nagrody": hash_nagrody,
                "hash_dymki": hash_dymki,
                "hash_gossip": hash_gossip
            }
        ).scalar_one()

        return tech_id