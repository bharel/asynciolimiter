@echo off
pushd %~dp0..\
call .\venv\Scripts\activate.bat
coverage run --branch -m unittest tests.py && coverage html
if NOT ["%errorlevel%"]==["0"] (
    pause
    exit /b %errorlevel%
)
popd