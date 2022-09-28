"""Support for a Balsam task server"""
from threading import Thread
from typing import Dict, Optional, Any
from time import sleep
from uuid import uuid4
import logging

from colmena.models import Result
from colmena.redis.queue import ClientQueues, TaskServerQueues
from colmena.task_server.base import BaseTaskServer


logger = logging.getLogger(__name__)

# TODO (wardlt): This is non-functional stub that must be filled in
class BalsamTaskServer(BaseTaskServer):
    """Implementation of a task server which executes applications registered with a Balsam workflow database"""

    def __init__(self,
                 queues: TaskServerQueues,
                 login_creds: Any,
                 pull_frequency: float,
                 aliases: Optional[Dict[str, str]] = None,
                 timeout: Optional[int] = None):
        """

        Args:
            aliases: Mapping between simple names and names of application for Balsam
            pull_frequency: How often to check for new tasks
            queues: Queues used to communicate with thinker
            timeout: Timeout for requests from the task queue
        """
        super().__init__(queues, timeout)
        self.aliases = aliases.copy()
        self.pull_frequency = pull_frequency

        # Store the user credentials
        self.login_creds = login_creds
        self.server_id = str(uuid4())

        # Ongoing tasks
        self.ongoing_tasks: Dict[str, Result] = dict()

        # Placeholder for a client objects
        self.balsam_client: Optional[Any] = None

    def process_queue(self, topic: str, task: Result):
        # TODO (wardlt): Send the task to Balsam

        # Get the name of the method
        app_name = self.aliases.get(task.method, task.method)
        task_id = self.balsam_client.send(
            inputs=(task.args, task.kwargs),
            method=app_name,
            nodes=task.resources.node_count,
            tags={'topic': topic, 'server_id': self.server_id, 'colmena_task_id': task.task_id}
        )
        logger.info(f'Submitted a {app_name} task to Balsam: {task_id}')

    def _query_results(self):
        """Runs and gets results that were submitted by th"""

        while True:
            # Query Balsam for completed tasks
            sleep(self.pull_frequency)
            new_tasks = self.balsam_client.get_tasks(server_id=self.server_id)

            # Send the completed tasks back
            for task in new_tasks:
                # Get the associated Colmena task
                result = self.ongoing_tasks.pop(task.colmena_task_id)

                # Add the result to the Colmena message and serialize it all
                result.set_result(task.result, task.runtime)
                result.serialize()

                # Send it back to the client
                self.queues.send_result(result, task.topic)

    def _setup(self):
        # TODO (wardlt): Prepare to send and receive tasks from Balsam
        # Connect to Balsam
        self.balsam_client = self.login_creds

        # Launch a thread
        return_thread = Thread(target=self._query_results, daemon=True)
        return_thread.start()
