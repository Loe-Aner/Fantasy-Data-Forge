-- badania pokazuja ze uzycie lokalnej kopii funkcji zamiast globalnej moze przyspieszyc kod o 30%

--[[
Kazda tablica sklada sie z dwoch czesci:
    a. czesc tablicowa (array part): tutaj sa wartosci indeksowane, 1,2,3... dostep do nich jest blyskawiczny (O(1)) poniewaz komputer wie dokladnie, gdzie w pamieci lezy element numer 5 (adres startowy + 5).
    b. czesc asocjacyjna (hash part): dziala jak slownik w pythonie. Trafia tu wszystko inne co w pkt a. Jest wolniejsza i zuzywa wiecej pamieci, bo musi:
       - zapisac klucz,
       - zapisac wartosc,
       - obliczyc hash by znalezc miejsce w pamieci.
--]]

function MojaFunkcja()
    print("Czesc")
end
MojaFunkcja()

-- powyzsze to tzw lukier skladniowy.
-- ponizsze jest analogicznie i to sie dzieje pod spodem
MojaFunkcja = function() print("Czesc") end
MojaFunkcja()

