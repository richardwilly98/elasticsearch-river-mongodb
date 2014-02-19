echo.
:LOOP
echo.
%MONGO_HOME%\bin\mongo < test-import-document.js
echo Waiting For 5 minutes... 
TIMEOUT /T 300 /NOBREAK
echo.
GOTO LOOP
