import os
import logging.config
import time
import boto3
import concurrent.futures
import Module.GetFiles.getfiles as getfiles
import pandas as pd
import Module.Logs.logs as log
import urllib.parse


abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

route = r"/home/ec2-user/interchange/main.py"

AWS_REGION = "us-east-1"

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/818835242461/app-interchange-"

def get_message(client: str) -> str:
	"""get_message method, its main role is to read the differet queues and get the message with the information to process

	Args:
			client (str): client code.

	Returns:
			message: exit message
	"""
	try:
		queue_name = "app-interchange-%s-sqs-%s" % (client.lower(), env_type)
		queue = sqs.get_queue_by_name(QueueName=queue_name)
		mssg_in_flight = queue.attributes.get("ApproximateNumberOfMessagesNotVisible")
		if mssg_in_flight == "0":
			try:
				response = sqs_client.receive_message(
					QueueUrl=QUEUE_URL + client.lower() + "-sqs-" + env_type,
					MaxNumberOfMessages=1,
					WaitTimeSeconds=10,
				)
				if len(response.get("Messages", [])) > 0:
					messages = response.get("Messages", [])
					message = messages[0]
					return message
				else:
					return None
			except:
				None
		else:
			return None
	except:
		None


def delete_sqs_message(receipt_handle: str, client: str) -> None:
	"""Delete received message from queue"""
	sqs_client.delete_message(
		QueueUrl=QUEUE_URL + client.lower() + "-sqs-" + env_type,
		ReceiptHandle=receipt_handle,
	)
	return None


def get_args(message) -> tuple:
	"""get arguments (file and client) to process"""
	body = message["Body"]
	arg1 = body.split(",")[1].split(" ")[1].split("/")[0]
	arg2 = body.split(",")[1].split(" ")[1].split("/")[-1]
	return arg1, arg2


def process_task(
	arg1: str, arg2: str, receipt_handle: str, client: str, log_name
) -> None:
	"""Launch the task

	Args:
			arg1 (str): clients code.
			arg2 (str): file path.
			receipt_handle (str): receipt handle.
			client: client code.
	"""

	client_code = arg1
	filename = urllib.parse.unquote(arg2)
	command = f"python {route} interpretation -c {client_code} -f {filename}"

	log.logs().exist_file(
		"OPERATIONAL",
		"INTELICA",
		"VISA AND MASTERCARD",
		log_name,
		"RUNNING ORCHESTRATOR",
		"INFO",
		f"{client}: starting process with file raw {arg2} and command {command}",
		module_name,
	)

	os.system(command)
	time.sleep(30)
	delete_sqs_message(receipt_handle, client)

	log.logs().exist_file(
		"OPERATIONAL",
		"INTELICA",
		"VISA AND MASTERCARD",
		log_name,
		"RUNNING ORCHESTRATOR",
		"INFO",
		f"{client}: Finishing process",
		module_name,
	)
	time.sleep(30)
	return None


if __name__ == "__main__":
	"""Executor for threads for sqs listener."""
	num_threads = 4
	module_name = "ORCHESTRATOR"
	sqs_client = boto3.client(
	"sqs",
	region_name=AWS_REGION,
	aws_access_key_id="AKIA35JTCHXOT4RKHYQR",
	aws_secret_access_key="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",
	)

	sqs = boto3.resource(
	"sqs",
	region_name=AWS_REGION,
	aws_access_key_id="AKIA35JTCHXOT4RKHYQR",
	aws_secret_access_key="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",
	)

	get_files = getfiles.get_files()

	list_of_clients = get_files.get_clients()

	df_client = pd.DataFrame(list_of_clients)

	clients = df_client.code.tolist()

	env_type = os.getenv("ENV_TYPE")
	
	log_name = log.logs().new_log(
		"OPERATIONAL",
		"",
		"INTELICA",
		"ACTIVATING ORCHESTRATOR",
		"SYSTEM",
		module_name,
	)
	with concurrent.futures.ThreadPoolExecutor(num_threads) as executor:
		futures = []
		while True:
			for client in clients:
				if len(futures) < num_threads:
					message = get_message(client)
					if message is None:
						None
					else:
						arg1, arg2 = get_args(message)
						future = executor.submit(
							process_task,
							arg1,
							arg2,
							message["ReceiptHandle"],
							client,
							log_name,
						)
						futures.append(future)

				else:
					done, futures = concurrent.futures.wait(
						futures, return_when=concurrent.futures.FIRST_COMPLETED
					)
					futures = list(futures)
			time.sleep(60)
