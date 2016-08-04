SET DB_PATH="D:\data\mongodb\rep1-db1"
mkdir %DB_PATH%
"%MONGO_HOME%\bin\mongod.exe" --replSet replica1 --port 27017 --dbpath %DB_PATH% --rest --oplogSize 100