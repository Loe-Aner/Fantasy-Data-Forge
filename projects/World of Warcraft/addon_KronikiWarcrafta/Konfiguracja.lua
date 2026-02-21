--[[ 
te trzy kropki to tzw. vararg expression
kiedy silnik wowa laduje plik .lua, robi to mniej wiecej tak:
    UruchomSkrypt("Konfiguracja.lua", "NazwaMojegoAddonu", "TabelaPrywatna")

Wewnatrz pliku lua, te trzy kropki przechwytują te dwa argumenty wyslane przez silnik.

Ta tabela w drugim argumencie jest tym samym obiektem w pamięci dla kazdego pliku w moim .toc
-- ]]
local nazwaAddonu, prywatna_tabela = ...

prywatna_tabela["AKTUALNA_WERSJA_DB"] = 1
--prywatna_tabela["DEBUGOWANKO"] = true
prywatna_tabela["FontTytulu"]  = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\MorpheusPL.ttf"
prywatna_tabela["FontTresci"]  = "Interface\\AddOns\\addon_KronikiWarcrafta\\Media\\FrizQuadrataPL.ttf"