#!/usr/bin/env python3
"""
Slave Process for Task Execution

This script is part of a fixed slave repository hosted at:
https://github.com/vitor-bfs/slave-repo

Repository Structure:
    ├── slave.py         # This file
    └── tasks/
         ├── task1/
         │     └── task1.py   # Must define a run() function as its entry point
         └── task2/
               └── task2.py   # And so on for each task

Overview:
    1. Loads configuration from config.yaml based on the TASK_TYPE environment variable.
    2. Dynamically loads and executes the specified task from the local tasks/ folder.
    3. Sends runtime log messages (e.g., warnings, errors) to a designated SQS queue.
    4. (Placeholder) Prepares for CloudWatch log integration.
    5. Sends a final completion signal ("DONE" on success, "ERROR" on failure) via SQS.
"""

import os
import sys
import logging
import boto3
import yaml
import importlib.util

# Configure basic logging.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class SQSSenderHandler(logging.Handler):
    """
    Custom logging handler that sends log messages to an SQS queue.

    Attributes:
        queue_url (str): The URL of the SQS queue where log messages are sent.
    """
    def __init__(self, queue_url):
        """
        Initialize the SQSSenderHandler.

        Args:
            queue_url (str): The SQS queue URL.
        """
        super().__init__()
        self.queue_url = queue_url
        self.sqs = boto3.client("sqs", region_name="us-east-2")

    def emit(self, record):
        """
        Emit a log record to the SQS queue.

        Args:
            record (logging.LogRecord): The log record to be sent.
        """
        try:
            msg = self.format(record)
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=msg)
        except Exception:
            self.handleError(record)


def send_completion_signal(queue_url: str, msg: str):
    """
    Send a final completion signal to the SQS queue.

    Args:
        queue_url (str): The SQS queue URL.
        msg (str): The message to send (e.g., "DONE" or "ERROR").
    """
    logging.info(f"Sending completion signal '{msg}' to SQS queue: {queue_url}")
    try:
        sqs = boto3.client("sqs", region_name="us-east-2")
        sqs.send_message(QueueUrl=queue_url, MessageBody=msg)
    except Exception as e:
        logging.error(f"Failed to send completion signal: {e}")


def execute_task(task_name: str) -> bool:
    """
    Dynamically load and execute a task module.

    The task module should be located at tasks/<task_name>/<task_name>.py and must
    define a `run()` function as its entry point.

    Args:
        task_name (str): The name of the task to execute.

    Returns:
        bool: True if the task executed successfully; False otherwise.
    """
    task_file = os.path.join("tasks", task_name, f"{task_name}.py")
    if not os.path.exists(task_file):
        logging.error(f"Task file does not exist: {task_file}")
        return False

    try:
        spec = importlib.util.spec_from_file_location(task_name, task_file)
        task_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(task_module)
    except Exception as e:
        logging.exception(f"Failed to load task module '{task_name}': {e}")
        return False

    if not hasattr(task_module, "run"):
        logging.error(f"Task module '{task_name}' does not define a run() function.")
        return False

    try:
        logging.info(f"Executing task '{task_name}'...")
        task_module.run()
        logging.info(f"Task '{task_name}' executed successfully.")
        return True
    except Exception as e:
        logging.exception(f"An error occurred during execution of task '{task_name}': {e}")
        return False


def main():
    """
    Main entry point for the slave process.

    Workflow:
        1. Loads configuration from config.yaml.
        2. Sets up SQS logging and (placeholder) CloudWatch logging.
        3. Determines the task to execute based on the TASK_TYPE environment variable.
        4. Dynamically executes the specified task.
        5. Sends a final completion signal to the SQS queue.
    """
    logging.info("Slave process starting up...")

    # Retrieve necessary environment variables.
    instance_id = os.environ.get("INSTANCE_ID", "unknown_instance")
    queue_url = os.environ.get("SQS_QUEUE_URL")
    task_type = os.environ.get("TASK_TYPE", "default")

    if not queue_url:
        logging.error("SQS_QUEUE_URL environment variable is not set.")
        sys.exit(1)

    # Attach the SQS log handler to send logs (INFO and above) to SQS.
    sqs_handler = SQSSenderHandler(queue_url)
    sqs_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    sqs_handler.setFormatter(formatter)
    logging.getLogger().addHandler(sqs_handler)

    # Placeholder for future CloudWatch logging integration.
    logging.info("CloudWatch logging integration: [Placeholder]")

    # Load configuration from config.yaml.
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logging.exception(f"Failed to load configuration: {e}")
        send_completion_signal(queue_url, "ERROR")
        sys.exit(1)

    # Retrieve the task configuration using the TASK_TYPE environment variable.
    task_config = config.get(task_type)
    if not task_config:
        logging.error(f"No configuration found for task type: {task_type}")
        send_completion_signal(queue_url, "ERROR")
        sys.exit(1)

    # Determine which task to execute from the configuration.
    task_name = task_config.get("task")
    if not task_name:
        logging.error("No task specified in the configuration.")
        send_completion_signal(queue_url, "ERROR")
        sys.exit(1)

    # Execute the task.
    success = execute_task(task_name)

    # Send the final completion signal.
    if success:
        send_completion_signal(queue_url, "DONE")
    else:
        send_completion_signal(queue_url, "ERROR")

    logging.info("Slave process completed all tasks. Shutting down.")


if __name__ == "__main__":
    main()
