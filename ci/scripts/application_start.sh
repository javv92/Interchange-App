#!/bin/bash
pwd
nohup ./interchange/Build/SH/orchestrator.sh > /dev/null 2>&1 &
sudo service logstash start
sudo service filebeat start