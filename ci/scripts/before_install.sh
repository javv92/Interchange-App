#!/bin/bash
pwd
crontab -l | grep -v './interchange/Build/SH/exchange_rate.sh'  | crontab  -