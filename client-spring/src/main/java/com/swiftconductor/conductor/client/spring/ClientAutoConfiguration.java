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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.netflix.discovery.EurekaClient;

import com.swiftconductor.conductor.client.automation.WorkerHost;
import com.swiftconductor.conductor.client.http.TaskClient;
import com.swiftconductor.conductor.client.worker.AbstractWorker;
import com.swiftconductor.conductor.sdk.worker.AnnotatedWorkerHost;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ClientProperties.class)
public class ClientAutoConfiguration {

    @Autowired(required = false)
    private EurekaClient eurekaClient;

    @Autowired(required = false)
    private List<AbstractWorker> workers = new ArrayList<>();

    @ConditionalOnMissingBean
    @Bean
    public TaskClient taskClient(ClientProperties clientProperties) {
        TaskClient taskClient = new TaskClient();
        taskClient.setRootURI(clientProperties.getRootUri());
        return taskClient;
    }

    @ConditionalOnMissingBean
    @Bean
    public AnnotatedWorkerHost annotatedWorkerHost(TaskClient taskClient) {
        return new AnnotatedWorkerHost(taskClient);
    }

    @ConditionalOnMissingBean
    @Bean(initMethod = "init", destroyMethod = "shutdown")
    public WorkerHost WorkerHost(TaskClient taskClient, ClientProperties clientProperties) {
        return new WorkerHost.Builder(taskClient, workers).withTaskThreadCount(clientProperties.getTaskThreadCount())
                .withThreadCount(clientProperties.getThreadCount())
                .withSleepWhenRetry((int) clientProperties.getSleepWhenRetryDuration().toMillis())
                .withUpdateRetryCount(clientProperties.getUpdateRetryCount())
                .withTaskToDomain(clientProperties.getTaskToDomain())
                .withShutdownGracePeriodSeconds(clientProperties.getShutdownGracePeriodSeconds())
                .withEurekaClient(eurekaClient).build();
    }
}
