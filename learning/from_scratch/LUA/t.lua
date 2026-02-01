-- print("Hello World!")

-- funkcja rekurencyjna liczaca silnie

-- definicja funkcji: "Tworzę nową funkcję o nazwie fact, która przyjmuje jeden argument o nazwie n"
function fact(n)
    if n == 0 then
        return 1
    else
        return n * fact(n-1)
    end
end

print("Wpisz liczbe: ...")
a = io.read("*number*")
-- io.read zatrzymuje program i czeka az user cos wpisze 
print(fact(a))

-- w lua zmienne sa domyslnie globalne, preferowane jest uzywanie local aby poprawic czytelnosc i wydajnosc


