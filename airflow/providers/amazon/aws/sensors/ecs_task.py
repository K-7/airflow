# @Author: K2A
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

import boto3

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class ECSTaskEndSensor(BaseSensorOperator):
    """
    This operator notifies when all ECS tasks end.
    Keeps checking for any running tasks and waits. Once all the ECS tasks end,
    the operator notifies and airflow task ends.

    Usecase: Trigger an action when ECS Tasks equals 0

    Requirement: Using Boto library for AWS interaction. Hence IAM role should have the permisssion
    or User Credentials provided needs to have sufficient permission

    :param cluster: ECS cluster name
    :param aws_config: Dict consisting of AWS list-tasks parameters
        aws_config = {
            containerInstance='string',
            family='string',
            nextToken='string',
            maxResults=123,
            startedBy='string',
            serviceName='string',
            desiredStatus='RUNNING' | 'PENDING' | 'STOPPED',
            launchType='EC2' | 'FARGATE'
        }
        These are the same parameters passed to Boto list_tasks function
    """
    template_fields = ('cluster', 'aws_config')
    ui_color = '#7c7287'

    @apply_defaults
    def __init__(self, cluster, aws_config=None, *args, **kwargs):
        self.aws_config = aws_config or {}
        self.aws_config['cluster'] = cluster
        self.ecs = boto3.client('ecs')
        super(ECSTaskEndSensor, self).__init__(*args, **kwargs)

    def poke(self, context):

        self.log.info('Check for ECS tasks: %s', self.aws_config['cluster'])
        response = self.ecs.list_tasks(**self.aws_config)

        self.log.info('No of tasks left: {0}'.format(len(response['taskArns'])))

        # If there are no Tasks
        if len(response['taskArns']) == 0:
            return True
        else:
            return False
