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
package com.swiftconductor.conductor.client.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.swiftconductor.conductor.client.worker.AbstractWorker;
import com.swiftconductor.conductor.common.metadata.tasks.Task;
import com.swiftconductor.conductor.common.metadata.tasks.TaskResult;

@SpringBootApplication
public class ExampleClient {

    public static void main(String[] args) {

        SpringApplication.run(ExampleClient.class, args);
    }

    @Bean
    public AbstractWorker worker() {
        return new AbstractWorker() {
            @Override
            public String getTaskDefName() {
                return "taskDef";
            }

            @Override
            public TaskResult execute(Task task) {
                return new TaskResult(task);
            }
        };
    }
}
