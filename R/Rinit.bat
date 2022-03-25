:: Rinit block
echo Reading config variables

for /f "delims=" %%x in (config.txt) do (set "%%x")

echo The following variables will be used
echo %r_path%

%r_path%\bin\RScript.exe  -e "install.packages('DBI', repos='https://cran.rstudio.com/')"&
%r_path%\bin\RScript.exe  -e "install.packages('data.table', repos='https://cran.rstudio.com/')"&
%r_path%\bin\RScript.exe  -e "install.packages('jsonlite', repos='https://cran.rstudio.com/')"&
%r_path%\bin\RScript.exe  -e "install.packages('mltools', repos='https://cran.rstudio.com/')"&
%r_path%\bin\RScript.exe  -e "install.packages('stringr', repos='https://cran.rstudio.com/')"&




