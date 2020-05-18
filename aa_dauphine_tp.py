# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import boto3





args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='dauphine2',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)


# [START howto_operator_python]
def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= "air-flow", Key= "petrol_consumption.csv")
    initial_df = pd.read_csv(obj['Body'])
    #s3.upload_fileobj(obj, "air-flow", "toto.csv")
    print(initial_df)
    return 'Whatever you return gets printed in the logas'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
# [END howto_operator_python]


# [START howto_operator_python_kwargs]
def my_sleeping_function(random_base):
    """This is a function that will run within the DAG execution"""
    #time.sleep(random_base)
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= "air-flow", Key= "petrol_consumption.csv")
    initial_df = pd.read_csv(obj['Body'])
    #s3.upload_fileobj(obj, "air-flow", "toto.csv")
    print(initial_df)


# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
#for i in range(5):
task = PythonOperator(
    task_id='toto',
    python_callable=my_sleeping_function,
    dag=dag,
    )

run_this >> task
# [END howto_operator_python_kwargs]
