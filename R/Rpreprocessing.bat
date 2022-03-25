:: Rpreprocessing block
echo Reading config variables
for /f "delims=" %%x in (config.txt) do (set "%%x")
echo Starting R preprocessing
%r_path%\bin\RScript.exe writers.R 
%r_path%\bin\RScript.exe directors.R 