def generuj_hash_djb2(tekst):
    if not tekst:
        return None

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