Scheme fee module
========================
Module for the scheme fee validations.


How to use the module
--------------------
This module is executed through the exec_scheme_fee. There is an obligation to define the client and the year and month to run in "Y%m%" format.It is executed following the line of code as an example:

```
exec_scheme_fee.py generate_table -c BRDRO -ym 202210
```

Also you can use to read a table in s3 repository to update the report of a given month with the updated data uploaded.
It is executed following the line of code as an example:

```
exec_scheme_fee.py read_table -c BRDRO -ym 202210 -f TEST11.csv 
```