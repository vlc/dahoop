set PATH=%BEDROCK%/bin;%PATH%
powershell -executionpolicy bypass "& """.\ci_win.ps1"""; exit $lastexitcode " 
