Ingest module
========================
The module has the purpose of processing the ARDEF parquet in the case of Visa while IAR in the case of Mastercard. Segment and load the data into the corresponding tables operational.dh_visa_ardef and operational.dh_mastercard_iar. In order to ensure the integrity of the information, it is analyzed based on the information already loaded, compared with the one that is being uploaded in order to update, inactivate or insert.

How to use the module
--------------------
The ingest module is already integrated in the main which involves the general execution of the process. This is seen in detail in the README.md of the main.