import time, random, string, os
from locust import HttpUser, task, between, tag, events, User
import locust_plugins
from azure.data.tables import TableClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError
from dotenv import load_dotenv

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--event-count", type=int, default=1)
    parser.add_argument("--table-name", type=str, default="TestTable")


class TableStorageUser(User):

    load_dotenv()
    host = os.getenv("HOST")
    entity = {
        "PartitionKey": "init",
        "RowKey": "init",
        "Data": os.getenv("JSON_DATA")
    }


    def create_entities(self):
        with TableClient.from_connection_string(os.getenv("CONNECTION_STRING"), self.environment.parsed_options.table_name) as table_client:

            # Create a table in case it does not already exist
            try:
                table_client.create_table()
            except HttpResponseError:
                print("Table already exists")

            # [START create_entity]
            request_meta = {
                "request_type": "http",
                "name": "create_entities",
                "start_time": time.time(),
                "response_length": 0,
                "exception": None,
                "context": None,
                "response": None,
            }
            for idx in range(self.environment.parsed_options.event_count):
                start_perf_counter = time.perf_counter()
                try:
                    self.entity["PartitionKey"] = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 30))
                    self.entity["RowKey"] = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 30))
                    
                    request_meta["response"] = table_client.create_entity(entity=self.entity)
                    request_meta["response_length"] = len(request_meta["response"])
                    
                    print(request_meta["response"])
                    
                except ResourceExistsError as e:
                    request_meta["exception"] = e
                    print("Entity already exists")

                request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
                self.environment.events.request.fire(**request_meta)    
        # [END create_entity]


    @task
    def create_test_data(self):
        self.create_entities()


