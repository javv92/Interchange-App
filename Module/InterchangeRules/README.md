Interchange rules module
========================
The module has the purpose of updating the rules that will be applied to the exchange process. This information will be loaded into the adapter table.


<!-- How to use the module
--------------------
To execute this module, the direct execution of InterchangeRules is used. To apply it to a process it is necessary to follow the following steps:
1. Update in the path the file of the new master with the name that appears in the repository
2. Access the console and execute the following:

```
cd ~/interchange
```

```
source interchange_env/bin/activate
```

3. Execute depending on the type of rules that you want to update, which are Visa or Mastercard

- Visa
```
python Module/InterchangeRules/InterchangeRules.py -c TI -brd VI
```

- Mastercard
```
 python Module/InterchangeRules/InterchangeRules.py -c TI -brd MC
``` -->
