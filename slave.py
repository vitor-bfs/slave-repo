#!/usr/bin/env python3
"""
Slave Process for Task Execution

This script is part of a fixed slave repository hosted at:
https://github.com/vitor-bfs/slave-repo

Repository Structure:
    ├── slave.py              # This file
    ├── dependencies.yaml     # Contains task dependency definitions
    ├── config.yaml           # Contains task configurations
    └── tasks/
         ├── task1/
         │     └── task1.py   # Must define a run() function as its entry point
         └── task2/
               └── task2.py   # Must define a run() function as its entry point

Overview:
    1. Loads configuration from config.yaml.
    2. Checks the TASK_TYPE environment variable:
         - If TASK_TYPE is "all", loads dependencies.yaml, computes the task execution order, and runs tasks sequentially.
         - Otherwise, executes a single specified task.
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
    """
    def __init__(self, queue_url):
        super().__init__()
        self.queue_url = queue_url
        self.sqs = boto3.client("sqs", region_name="us-east-2")

    def emit(self, record):
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


def topological_sort(tasks, dependencies):
    """
    Compute a topological order of tasks given their dependencies.

    Args:
        tasks (set): A set of task names.
        dependencies (dict): A dictionary where key is a task and value is a list of tasks it depends on.

    Returns:
        list: A list of tasks in execution order.
    """
    from collections import defaultdict, deque

    # Build graph and in-degree count.
    graph = defaultdict(list)
    in_degree = {task: 0 for task in tasks}
    for task, deps in dependencies.items():
        for dep in deps:
            graph[dep].append(task)
            in_degree[task] += 1

    # Find tasks with zero in-degree.
    queue = deque([task for task in tasks if in_degree[task] == 0])
    order = []

    while queue:
        current = queue.popleft()
        order.append(current)
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(order) != len(tasks):
        logging.error("Cycle detected or missing tasks in dependencies. Cannot compute a valid execution order.")
        return []

    return order


def execute_task_sequence(task_order):
    """
    Execute tasks in the provided order sequentially.

    Args:
        task_order (list): List of task names in the order they should be executed.

    Returns:
        bool: True if all tasks executed successfully, False otherwise.
    """
    for task in task_order:
        success = execute_task(task)
        if not success:
            logging.error(f"Execution halted. Task '{task}' failed.")
            return False
    return True


def main():
    """
    Main entry point for the slave process.

    Workflow:
        1. Loads configuration from config.yaml.
        2. Checks the TASK_TYPE environment variable:
             - If TASK_TYPE is "all", loads dependencies.yaml, computes execution order, and runs tasks sequentially.
             - Otherwise, executes a single specified task.
        3. Sends a final completion signal to the SQS queue.
    """
    logging.info("Slave process starting up...")

    instance_id = os.environ.get("INSTANCE_ID", "unknown_instance")
    queue_url = os.environ.get("SQS_QUEUE_URL")
    task_type = os.environ.get("TASK_TYPE", "default")

    if not queue_url:
        logging.error("SQS_QUEUE_URL environment variable is not set.")
        sys.exit(1)

    # Attach the SQS log handler.
    sqs_handler = SQSSenderHandler(queue_url)
    sqs_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    sqs_handler.setFormatter(formatter)
    logging.getLogger().addHandler(sqs_handler)

    logging.info("CloudWatch logging integration: [Placeholder]")

    # Load task configuration from config.yaml.
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logging.exception(f"Failed to load configuration: {e}")
        send_completion_signal(queue_url, "ERROR")
        sys.exit(1)

    if task_type == "all":
        # Load dependencies from dependencies.yaml.
        try:
            with open("dependencies.yaml", "r") as f:
                deps_config = yaml.safe_load(f)
            dependencies = deps_config.get("dependencies", {})
        except Exception as e:
            logging.exception(f"Failed to load dependencies: {e}")
            send_completion_signal(queue_url, "ERROR")
            sys.exit(1)

        # Determine all tasks from configuration.
        all_tasks = set(config.keys())
        task_order = topological_sort(all_tasks, dependencies)
        if not task_order:
            send_completion_signal(queue_url, "ERROR")
            sys.exit(1)

        logging.info(f"Computed task execution order: {task_order}")
        success = execute_task_sequence(task_order)
    else:
        # Single task execution.
        task_config = config.get(task_type)
        if not task_config:
            logging.error(f"No configuration found for task type: {task_type}")
            send_completion_signal(queue_url, "ERROR")
            sys.exit(1)
        single_task = task_config.get("task")
        if not single_task:
            logging.error("No task specified in the configuration.")
            send_completion_signal(queue_url, "ERROR")
            sys.exit(1)
        success = execute_task(single_task)

    # Send final completion signal.
    if success:
        send_completion_signal(queue_url, "DONE")
    else:
        send_completion_signal(queue_url, "ERROR")

    logging.info("Slave process completed all tasks. Shutting down.")


if __name__ == "__main__":
    main()
