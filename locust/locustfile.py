from locust import HttpUser, TaskSet, task, between, events
import random

class APICalls(TaskSet):    

    @events.init_command_line_parser.add_listener
    def _(parser):
        parser.add_argument("--my-argument", type=str, default="", help="It's working")


    @events.test_start.add_listener
    def _(environment, **kw):
        print(f"Custom argument supplied: {environment.parsed_options.my_argument}")




    @task()
    def getpost(self):
        post_id = random.randint(1, 100)
        self.client.get("/posts/%i" % post_id, name="/posts/[id]")

    @task()
    def postpost(self):        
        self.client.post("/posts", {"title": "foo", "body": "bar", "userId": 1}, name="/posts")

class APIUser(HttpUser):
    tasks = [APICalls]
    wait_time = between(5, 10) # seconds