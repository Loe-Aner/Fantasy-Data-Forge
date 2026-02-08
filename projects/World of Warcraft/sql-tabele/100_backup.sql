DECLARE @sciezka_pliku NVARCHAR(256);
DECLARE @data_tekst NVARCHAR(20);

SET @data_tekst = FORMAT(GETDATE(), 'ddMMyyyy');
SET @sciezka_pliku = 'D:\MyProjects_4Fun\projects\World of Warcraft\_db_backup\WoW_PL_backup_' + @data_tekst + '.bak';

BACKUP DATABASE WoW_PL
TO DISK = @sciezka_pliku
WITH FORMAT,
MEDIANAME = 'KopiaSQL',
NAME = 'Pelna Kopia Bazy Danych';