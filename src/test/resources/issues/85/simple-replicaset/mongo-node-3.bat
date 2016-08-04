SET DB_PATH="D:\data\mongodb\rep1-db3"
mkdir %DB_PATH%
"%MONGO_HOME%\bin\mongod.exe" --replSet replica1 --port 27019 --dbpath %DB_PATH% --rest --oplogSize 100