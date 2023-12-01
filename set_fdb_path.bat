@echo off

rem Define the path to the fdb library.
set fdb_path="C:\Program Files\fdb\bin"

rem Add the path to the fdb library to the PATH variable.
set PATH=%PATH%;%fdb_path%

rem Print the new PATH variable.
echo %PATH%