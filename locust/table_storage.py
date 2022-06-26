import time, random, string, os, inspect, gevent
import locust_plugins

from locust.runners import STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP, WorkerRunner
from locust import HttpUser, task, between, tag, events, User
from azure.data.tables import TableClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError
from dotenv import load_dotenv

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--event-count", type=int, default=1)
    parser.add_argument("--event-object-id", type=str, default="*")
    parser.add_argument("--insert-table", type=str, default="TestTable")
    parser.add_argument("--move-from-table", type=str, default="")
    parser.add_argument("--move-to-table", type=str, default="")

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if not isinstance(environment.runner, WorkerRunner):
        gevent.spawn(check_total_requests_reached, environment)

def check_total_requests_reached(environment):
    while not environment.runner.state in [STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP]:
        time.sleep(0.025)
        # print("total: ", environment.runner.stats.total.num_requests)
        if environment.runner.stats.total.num_requests >= environment.parsed_options.event_count:
            environment.runner.quit()
            return

class TableStorageUser(User):    
    ## TAKS
    
    # @task
    # def insert_events(self):
    #     self.create_entities()

    @task
    def move_events(self):
        self.query_entities()


    ## FUNCTIONS


    def __init__(self, environment):
        self.environment = environment
       
        load_dotenv()
        self.entity = {
            "PartitionKey": "init",
            "RowKey": "init",
            "Data": os.getenv("JSON_DATA")
        }
        self.table_client = TableClient.from_connection_string(os.getenv("CONNECTION_STRING"), self.environment.parsed_options.insert_table)
    
    def create_entities(self):
        # Create a table in case it does not already exist
        try:
            self.table_client.create_table()
        except HttpResponseError:
            print("Table already exists")

        # [START create_entity]
        self.entity["PartitionKey"] = self.environment.parsed_options.event_object_id
        request_meta = {
            "request_type": "http",
            "name": inspect.stack()[1].function,
            "start_time": time.time(),
            "response_length": 0,
            "exception": None,
            "context": None,
            "response": None,
        }
        
        for idx in range(self.environment.parsed_options.event_count):
            start_perf_counter = time.perf_counter()
            try:

                if self.entity["PartitionKey"] == '*':
                    self.entity["PartitionKey"] = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 30))
                
                self.entity["RowKey"] = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 30))
                
                request_meta["response"] = self.table_client.create_entity(entity=self.entity)
                request_meta["response_length"] = len(request_meta["response"])
                
                # print(request_meta["response"])
                
            except ResourceExistsError as e:
                request_meta["exception"] = e
                print("Entity already exists")

            request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
            self.environment.events.request.fire(**request_meta)    
        # [END create_entity]


    def query_entities(self):
        # [START query_entities]        
        try:
            parameters = {"eventObjectId": self.environment.parsed_options.event_object_id}
            name_filter = "PartitionKey eq @eventObjectId"
            queried_entities = self.table_client.query_entities(
                query_filter=name_filter, parameters=parameters
            )

            for entity_chosen in queried_entities:
                print("ROW KEY = " , entity_chosen["RowKey"])

        except HttpResponseError as e:
            print(e.message)
        # [END query_entities]




