# 📊 Funds Dashboard (Python + Power BI + Analizy SQL)

Projekt do analizy i wizualizacji rentowności funduszy inwestycyjnych.  
Dane źródłowe pochodzą z plików Excel/CSV oraz kart funduszu (PDF), które następnie są przetwarzane i wizualizowane w Power BI.  
Część informacji jest pozyskiwana automatycznie z plików PDF za pomocą **Gemini API**.

---

## 🎯 Cel projektu

Celem tego dashboardu jest **prezentacja i porównanie** wybranych funduszy inwestycyjnych na podstawie ogólnodostępnych danych rynkowych.  
Analiza obejmuje między innymi **stopy zwrotu** (narastające oraz kroczące w różnych horyzontach),  
a także **ryzyko** i **kluczowe charakterystyki** takie jak **wielkość aktywów netto**, **struktura aktywów**,  
**alokacja sektorowa**, **geograficzna** i **walutowa**.

Dzięki temu możliwe jest pełniejsze **zrozumienie profilu poszczególnych funduszy** – nie tylko tego, jak zmieniała się ich wartość w czasie,  
ale również **w jakich warunkach rynkowych funkcjonowały oraz jak kształtowała się ich ekspozycja na różne klasy aktywów**.

Dashboard ma wyłącznie charakter **informacyjno-analityczny**  
i **nie stanowi rekomendacji inwestycyjnej**.

---

## 📂 Struktura projektu

- **fundusze_get_data.ipynb** - główny notebook Jupyter
- **0. Dasboard - przyklady** - jak wygladaja poszczegolne strony
- **1. Aktywa netto/**
  - `aktywa.xlsx`  
- **2. Benchmark/**
  - `benchmark.xlsx`  
- **3. Struktura aktywow/**
  - `struktura_aktywow.xlsx`  
- **4. Struktura geograficzna/**
  - `struktura_geograficzna.xlsx`  
- **5. Najwieksze pozycje w portfelu/**
  - `najw_poz_w_porfelu.xlsx`  
- **6. Alokacja sektorowa/**
  - `alokacja_sektorowa.xlsx`  
- **7. Waluty/**
  - `waluty.xlsx`  
- **Additional Tables/**
  - `extraInfo.csv`  
- **Karty funduszu/** - podfoldery = tickery funduszy  
  - `MIL27/2025-04-30.pdf`  
  - `ING43/2025-07-31.pdf`
  - `.../....pdf`
- **Notowania/**
  - `dane.csv`
- **Przykładowe Miary w DAX/** - jak niektóre parametry są liczone
- **Przykładowe klauzule SQL (analizy)/** - analizy zbiorów z wykorzystaniem SQL'a
- **Zwrot od Gemini/** - odpowiedzi Gemini (per ticker)  
  - `MIL27/...`  
  - `ING43/...`
  - `.../...`
