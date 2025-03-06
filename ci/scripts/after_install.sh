#!/bin/bash
sudo chown -R ec2-user:ec2-user ~/interchange

cd ~/interchange

python3.10 -m venv interchange_env
source interchange_env/bin/activate

pip3.10 install -r Dep/requirements.txt
sudo chmod +x ./Build/SH/orchestrator.sh
sudo chmod +x ./Build/SH/exchange_rate.sh

sudo cp ./Config/filebeat/filebeat.yml /etc/filebeat/filebeat.yml
sudo cp ./Config/logstash/filebeat-intelica.conf /etc/logstash/conf.d/filebeat-intelica.conf
sudo cp ./Config/logstash/pipelines.yml /etc/logstash/pipelines.yml

(crontab -l 2>/dev/null || echo ""; echo "45 21 * * * ./interchange/Build/SH/exchange_rate.sh") | sort - | uniq - | crontab -

rm -r scripts && rm appspec.yml && rm -r Config

aws s3api put-object --bucket intelica-interchange-landing-%ENVIRONMENT% --key Intelica/INTERCHANGE_RULES/MASTERCARD/
aws s3api put-object --bucket intelica-interchange-landing-%ENVIRONMENT% --key Intelica/INTERCHANGE_RULES/VISA/
aws s3api put-object --bucket intelica-interchange-structured-%ENVIRONMENT% --key ADAPTERS/
