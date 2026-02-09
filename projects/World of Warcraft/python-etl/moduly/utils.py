def generuj_hash_djb2(tekst):
    """
    To jest pythonowy odpowiednik funkcji z Lua do hashowania.
    """
    if not tekst:
        return None

    hash_val = 5381
    
    for char in tekst:
        kod_znaku = ord(char)
        hash_val = (hash_val * 33) + kod_znaku 
        hash_val = hash_val & 0xFFFFFFFF
    return f"{hash_val:x}"

if __name__ == "__main__":
    przyklady = [
        ("Warming Up", "b135ed3f"),
        ("Stand Your Ground", "ec5d92dd"),
        ("Spar with Private Cole.", "36d30343"),
        ("Prepare yourself, {imie}. We're nearing the island where the previous expedition was lost.\r\n\r\nWarm up on these combat dummies. We need everyone ready for whatever we find on that island.", "90364fdd")
    ]

    print("\n" + "="*50)
    print(f"{'HASH Z LUA':<10} | {'HASH Z PYTHONA':<10} | {'WYNIK'}")
    print("="*50)

    for tekst, hash_lua in przyklady:
        hash_python = generuj_hash_djb2(tekst)
        
        if hash_python == hash_lua:
            czy_zgodne = "OK"
        else:
            czy_zgodne = "BŁĄD"
            
        print(f"{hash_lua:<10} | {hash_python:<10} | {czy_zgodne}")

    print("="*50 + "\n")
