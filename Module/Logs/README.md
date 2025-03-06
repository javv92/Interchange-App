Log module
==========
The module has the purpose of communicating to the user summary descriptions of what is happening in the execution of the process which includes the use of the other modules. These are stored in the control.t_log table as history and, regarding the execution at the moment, a file is generated that allows identifying what has been done.

How to use the module
--------------------
The logs module is already integrated into all the processes covered by the system. To apply it to a process it is necessary to follow the following steps:

1. Import the module with the following line

```
import Module.Logs.logs as log
```
2. Create a new log having the example as follows:
```
log_name = log.logs().new_log("EXCHANGE_RATE","","INTELICA", "GET VISA AND MASTERCARD EXCHANGE RATES","SYSTEM","EXCHANGE RATES")
```
3. Continuing with the steps of the log that has been created requires continuing with an existing existence of one with the following line of code for example:

```
log.logs().exist_file("EXCHANGE_RATE","INTELICA","VISA AND MASTERCARD",log_name, "getting exchange rates of the date ", "INFO","in process", "EXCHANGE RATES)

```