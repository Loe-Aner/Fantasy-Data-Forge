def generuj_hash_djb2(tekst):

    tekst = str(tekst)
    
    if not tekst:
        return None
    
    tekst = tekst.lower()

    hash_val = 5381
    hash_val_2 = 0
    
    for znak in tekst:
        kod_znaku = ord(znak)
        
        hash_val = (hash_val * 33 + kod_znaku) & 0xFFFFFFFF
        hash_val_2 = (hash_val_2 * 65599 + kod_znaku) & 0xFFFFFFFF
        
    return f"{hash_val:08x}{hash_val_2:08x}"

# if __name__ == "__main__":
#     przyklady = [
#         ("kikikiki123", "11d253cbea07cd3a"),
#         ("Slay 7 Quilboar within the Quilboar Briarpatch.", "be2dd595083a6236")
#     ]

#     print("\n" + "=" * 50)
#     print(f"{'HASH Z LUA':<18} | {'HASH Z PYTHONA':<18} | {'WYNIK'}")
#     print("=" * 50)

#     for tekst, hash_lua in przyklady:
#         hash_python = generuj_hash_djb2(tekst)
        
#         if hash_python == hash_lua:
#             czy_zgodne = "OK"
#         else:
#             czy_zgodne = "BŁĄD"
            
#         print(f"{hash_lua:<18} | {hash_python:<18} | {czy_zgodne}")

#     print("=" * 50 + "\n")

def sklej_warunki_w_WHERE(
    kraina: str | None = None, 
    fabula: str | None = None, 
    dodatek: str | None = None,
    id_misji: int | None = None
):
    if id_misji is not None:
        return "AND m.MISJA_ID_MOJE_PK = :id_misji"

    czesci_warunku = []
    
    if kraina is not None:
        czesci_warunku.append("AND m.KRAINA_EN = :kraina_en")
        
    if fabula is not None:
        czesci_warunku.append("AND m.NAZWA_LINII_FABULARNEJ_EN = :fabula_en")

    if dodatek is not None:
        czesci_warunku.append("AND m.DODATEK_EN = :dodatek_en")
    
    if czesci_warunku:
        return "\n        ".join(czesci_warunku)

    raise ValueError("Nie podano żadnych parametrów filtrowania (ID, Kraina, Fabuła lub Dodatek).")
