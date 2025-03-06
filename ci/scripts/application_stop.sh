#!/bin/bash
sudo pkill -f "orchestrator.py"
sudo service logstash stop
sudo service filebeat stop