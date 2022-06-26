#!/bin/bash

pip install python-dotenv

pip install azure-data-tables

pip install locust-plugins

locust --locustfile /home/locust/locust/table_storage.py ${*}