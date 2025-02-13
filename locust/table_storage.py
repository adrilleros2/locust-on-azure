import time, random, string, os, inspect, gevent

from locust.runners import STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP, WorkerRunner
from locust import HttpUser, task, between, tag, events, User, TaskSet
from azure.data.tables import TableClient, TableTransactionError
from azure.core.exceptions import ResourceExistsError, HttpResponseError
from dotenv import load_dotenv

expected_request_count = -1




@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--event-count", type=int, default=1)
    parser.add_argument("--event-object-id", type=str, default="*")
    parser.add_argument("--insert-table", type=str, default="TestTable")
    parser.add_argument("--move-from-table", type=str, default="")
    parser.add_argument("--move-to-table", type=str, default="")
    # parser.add_argument("--expected-request-count", type=int, default=1, include_in_web_ui=False)

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if not isinstance(environment.runner, WorkerRunner):
        gevent.spawn(check_total_requests_reached, environment)

def check_total_requests_reached(environment):
    global expected_request_count
    while not environment.runner.state in [STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP]:
        time.sleep(0.025)
        # print("total: ", environment.runner.stats.total.num_requests)
        if environment.runner.stats.total.num_requests >= expected_request_count:
            environment.runner.quit()
            return

class TableStorageTasks(TaskSet):    
    ## TASKS
    
    @task
    def insert_events(self):
        table_client = TableClient.from_connection_string(os.getenv("CONNECTION_STRING"), self.environment.parsed_options.insert_table)
        self.generate_entities(table_client)

    @task
    def move_events(self):
        source_table_client = TableClient.from_connection_string(os.getenv("CONNECTION_STRING"), self.environment.parsed_options.move_from_table)
        target_table_client = TableClient.from_connection_string(os.getenv("CONNECTION_STRING"), self.environment.parsed_options.move_to_table)

        self.move_entities(source_table_client, target_table_client)


class TableStorageUser(User):
   
    ## FUNCTIONS

    def __init__(self, environment):
        load_dotenv()
        self.environment = environment

        enable_move_events_task = self.neither_empty_nor_starts_with_hash(self.environment.parsed_options.move_from_table) \
            and self.neither_empty_nor_starts_with_hash(self.environment.parsed_options.move_from_table)

        global expected_request_count
        expected_request_count = 1 if enable_move_events_task else self.environment.parsed_options.event_count
        
        self.tasks = [TableStorageTasks.move_events if enable_move_events_task else TableStorageTasks.insert_events]
        # self.environment.tags = []
        
        self.entity = {
            "PartitionKey": "init",
            "RowKey": "init",
            "Data": os.getenv("JSON_DATA")
        }

    def neither_empty_nor_starts_with_hash(self, value):
        return len(value) > 0 and not(value.startswith("#"))
    
    def generate_entities(self, table_client):
        # Create a table in case it does not already exist
    
        try:
            table_client.create_table()
        except HttpResponseError:
            print("Table already exists")

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
                
                request_meta["response"] = table_client.create_entity(entity=self.entity)
                request_meta["response_length"] = len(request_meta["response"])
                
                
            except ResourceExistsError as e:
                request_meta["exception"] = e
                print("Entity already exists")

            request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
            self.environment.events.request.fire(**request_meta)    


    def query_entities(self, table_client):
        parameters = {"eventObjectId": self.environment.parsed_options.event_object_id}
        name_filter = "PartitionKey eq @eventObjectId"
        queried_entities = table_client.query_entities(
            query_filter=name_filter, parameters=parameters
        )

        return queried_entities


    def move_entities(self, source_table_client, target_table_client):
        request_meta = {
            "request_type": "http",
            "name": inspect.stack()[1].function,
            "start_time": time.time(),
            "response_length": 0,
            "exception": None,
            "context": None,
            "response": None,
        }
        
        start_perf_counter = time.perf_counter()
        
        try:
            entities = self.query_entities(source_table_client)
            
            delete_operations = []
            upsert_operations = []
            for entity in entities:
                delete_operations.append(("delete", entity))
                upsert_operations.append(("upsert", entity))        

            try:
                target_table_client.create_table()
            except HttpResponseError as e:
                print("Table already exists")

        
            target_table_client.submit_transaction(upsert_operations)
            # request_meta["response_length"] = get_deep_size(upsert_operations)

            source_table_client.submit_transaction(delete_operations)
            # request_meta["response_length"] += get_deep_size(delete_operations)

        except HttpResponseError as hre:
            request_meta["exception"] = hre
        except TableTransactionError as tte:
            request_meta["exception"] = tte

        
        request_meta["response_time"] = (time.perf_counter() - start_perf_counter) * 1000
        self.environment.events.request.fire(**request_meta)