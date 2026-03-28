DECLARE @backupPath NVARCHAR(500)
SET @backupPath = N'/var/opt/mssql/backups/jardineria_FULL_20260327.bak'

BACKUP DATABASE [jardineria]
TO DISK = @backupPath
WITH FORMAT, 
     MEDIANAME = 'JardineriaBackup',
     NAME = 'Backup Completo - jardineria',
     COMPRESSION, 
     CHECKSUM, 
     STATS = 10;

DECLARE @ruta NVARCHAR(500);
DECLARE @fecha VARCHAR(8) = CONVERT(VARCHAR, GETDATE(), 112);
SET @ruta = N'/var/opt/mssql/backups/jardineria_staging_FULL_' + @fecha + '.bak';

BACKUP DATABASE [jardineria_staging]
TO DISK = @ruta 
WITH FORMAT, 
     COMPRESSION, 
     CHECKSUM, 
     STATS = 10;