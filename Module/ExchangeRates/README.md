Exchange rate module
========================
Module used to extract the exchange rates of the day or some referential date of Visa and Mastercard. It should be noted that to obtain the exchange rate date of the day, the process must be run after 3:05 p.m. UTC-5. It's currently scheduled to run automatically at 4:45 p.m. to avoid wrong data.

How to use the module
--------------
This module is executed through the exec_exchange_rates. It's necessary to execute the following line of code depending on what the user wants, for example:

Extract data with reference to a date
```
python -u ~/interchange/exec_exchange_rates.py exchange_rates -d 2022-11-15
```

Extract the data of the same day knowing that it is necessary to run it after 3:05 p.m. UTC-5
```
python -u ~/interchange/exec_exchange_rates.py exchange_rates
```