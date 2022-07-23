@echo off
pushd %~dp0..\
call .\venv\Scripts\activate.bat
coverage run --branch -m unittest tests.py && coverage html && start explorer .\htmlcov\index.html
if NOT ["%errorlevel%"]==["0"] (
    pause
    exit /b %errorlevel%
)
popd