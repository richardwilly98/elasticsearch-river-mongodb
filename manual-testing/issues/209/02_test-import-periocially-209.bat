echo.
:LOOP
echo.
%MONGO_HOME%\bin\mongo < test-import-document.js
echo Waiting For 5 minutes... 
TIMEOUT /T 180 /NOBREAK
echo.
GOTO LOOP
