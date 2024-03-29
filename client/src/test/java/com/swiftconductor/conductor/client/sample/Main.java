/*
 * Copyright 2023 Swift Software Group, Inc.
 * (Code and content before December 13, 2023, Copyright Netflix, Inc.)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.swiftconductor.conductor.client.sample;

import java.util.Arrays;

import com.swiftconductor.conductor.client.automation.WorkerHost;
import com.swiftconductor.conductor.client.http.TaskClient;
import com.swiftconductor.conductor.client.worker.AbstractWorker;

public class Main {

    public static void main(String[] args) {

        TaskClient taskClient = new TaskClient();
        taskClient.setRootURI("http://localhost:8080/api/"); // Point this to the server API

        int threadCount = 2; // number of threads used to execute workers. To avoid starvation, should be
        // same or more than number of workers

        AbstractWorker worker1 = new SampleWorker("task_1");
        AbstractWorker worker2 = new SampleWorker("task_5");

        // Create WorkerHost
        WorkerHost host = new WorkerHost.Builder(taskClient, Arrays.asList(worker1, worker2))
                .withThreadCount(threadCount).build();

        // or
        // WorkerHost host = new WorkerHost.Builder(taskClient, Arrays.asList(worker1,
        // worker2))
        // .withTaskThreadCount(Map.of("task_1", 1, "task_5", 1)).build();

        // Start the polling and execution of tasks
        host.init();
    }
}
