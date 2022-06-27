import time, random, string, os, inspect, gevent, uuid

from locust.runners import STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP, WorkerRunner
from locust import HttpUser, task, events, User, TaskSet
from azure.storage.fileshare import ShareClient, ShareFileClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError
from dotenv import load_dotenv
from datetime import date


expected_request_count = -1


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--event-count", type=int, default=1)
    parser.add_argument("--event-object-id", type=str, default="")
    parser.add_argument("--move-to-file-share", type=str, default="")

# @events.test_start.add_listener
# def on_test_start(environment, **kwargs):
#     if not isinstance(environment.runner, WorkerRunner):
#         gevent.spawn(check_total_requests_reached, environment)
#
#
# def check_total_requests_reached(environment):
#     global expected_request_count
#     while not environment.runner.state in [STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP]:
#         time.sleep(0.025)
#         # print("total: ", environment.runner.stats.total.num_requests)
#         if environment.runner.stats.total.num_requests >= environment.parsed_options.event_count:
#             environment.runner.quit()
#             return


class FileShareTasks(TaskSet):
    ## TASKS

    @task
    def generate_events(self):
        file_share_client = ShareClient.from_connection_string(os.getenv("CONNECTION_STRING"), "events-dtl-" + date.today().isoformat())
        self.generate_events(file_share_client)

    @task
    def move_events(self):
        source_file_share_client = ShareClient.from_connection_string(os.getenv("CONNECTION_STRING"), "events-dtl-" + date.today().isoformat())
        target_file_share_client = ShareClient.from_connection_string(os.getenv("CONNECTION_STRING"), self.environment.parsed_options.move_to_file_share)

        self.move_events(source_file_share_client, target_file_share_client)

class FileShareUser(User):
   
    ## FUNCTIONS

    def __init__(self, environment):
        load_dotenv()
        self.environment = environment

        enable_move_events_task = not self.empty_or_starts_with_hash(self.environment.parsed_options.move_to_file_share)

        # global expected_request_count
        # expected_request_count = 1 if enable_move_events_task else self.environment.parsed_options.event_count

        self.tasks = [FileShareTasks.move_events if enable_move_events_task else FileShareTasks.generate_events]

    def empty_or_starts_with_hash(self, value):
        return len(value) == 0 or value.startswith("#")
    
    def generate_events(self, file_share_client):
        request_meta = {
            "request_type": "http",
            "name": inspect.stack()[1].function,
            "start_time": time.time(),
            "response_length": 0,
            "exception": None,
            "context": None,
            "response": None,
        }

        try:
            file_share_client.create_share()
            print("Fileshare created")
        except ResourceExistsError:
            print("Fileshare already exists")

        for idx in range(self.environment.parsed_options.event_count):
            start_perf_counter = time.perf_counter()

            event_object_id = self.environment.parsed_options.event_object_id
            if self.empty_or_starts_with_hash(event_object_id):
                event_object_id = str(uuid.uuid4()).upper()

            event_object_directory = file_share_client.get_directory_client(event_object_id)
            if not event_object_directory.exists():
                print("Creating directory")
                event_object_directory.create_directory()
            else:
                print("Directory already exists")

            file_name = str(uuid.uuid4()).upper() + ".json"
            event_object_directory.upload_file(file_name, os.getenv("JSON_DATA"))

            request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
            self.environment.events.request.fire(**request_meta)

    def move_events(self, source_file_share_client, target_file_share_client):
        target_directory = self.create_file_share_and_directory(target_file_share_client, self.environment.parsed_options.event_object_id)

        source_directory = source_file_share_client.get_directory_client(self.environment.parsed_options.event_object_id)
        files_to_move = source_directory.list_directories_and_files()

        for file in files_to_move:
            source_url = "https://{}.file.core.windows.net/{}/{}".format(
                os.getenv("ACCOUNT_NAME"),
                source_file_share_client.share_name,
                source_directory.directory_path + "/" + file["name"]
            )

            destination_file = target_directory.get_file_client(file["name"])
            destination_file.start_copy_from_url(source_url)

            source_file_client = source_directory.get_file_client(file["name"])
            source_file_client.delete_file()

        source_directory.delete_directory()

    def create_file_share_and_directory(self, file_share_client, directory_name):
        try:
            file_share_client.create_share()
            print("Fileshare created")
        except ResourceExistsError:
            print("Fileshare already exists")

        directory = file_share_client.get_directory_client(directory_name)
        if not directory.exists():
            print("Creating directory")
            directory.create_directory()
        else:
            print("Directory already exists")

        return directory
