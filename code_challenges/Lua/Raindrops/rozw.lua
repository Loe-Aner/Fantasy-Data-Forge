local liczba = 22
local wynik = ""

if liczba % 3 == 0 then
    wynik = wynik .. "Pling"
end

if liczba % 5 == 0 then
    wynik = wynik .. "Plang"
end

if liczba % 7 == 0 then
    wynik = wynik .. "Plong"
end

if wynik == "" then
    wynik = tostring(liczba)
end

print(wynik)