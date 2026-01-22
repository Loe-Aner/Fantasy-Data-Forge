from moduly.etl_excel import zapisz_npc_i_status_przetlumaczony_do_db 
from moduly.db_core import utworz_engine_do_db

# TO ZAPISUJE PRZETLUMACZONEG NPC'A DO DB (JEGO NAZWE) TYLKO TYCH KTORYCH NIE MA W DB
# ROBI TEZ UPDATE PARAMETROW (CALOSC ZA KAZDYM RAZEM) W dbo.NPC (CZYLI PLEC, RASA, ITP)
# OSTATNIE REVIEW: 22.01.2026 -> brak zmian
zapisz_npc_i_status_przetlumaczony_do_db(
    silnik=utworz_engine_do_db()
    )