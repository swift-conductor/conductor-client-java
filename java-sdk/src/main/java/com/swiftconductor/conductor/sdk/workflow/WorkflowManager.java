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
package com.swiftconductor.conductor.sdk.workflow;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.ClientHandler;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.swiftconductor.conductor.client.http.MetadataClient;
import com.swiftconductor.conductor.client.http.TaskClient;
import com.swiftconductor.conductor.client.http.WorkflowClient;
import com.swiftconductor.conductor.common.metadata.tasks.TaskDef;
import com.swiftconductor.conductor.common.metadata.tasks.TaskType;
import com.swiftconductor.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.swiftconductor.conductor.common.metadata.workflow.WorkflowDef;
import com.swiftconductor.conductor.common.run.Workflow;
import com.swiftconductor.conductor.sdk.workflow.def.WorkflowWithInput;
import com.swiftconductor.conductor.sdk.workflow.def.tasks.*;
import com.swiftconductor.conductor.sdk.workflow.utils.ObjectMapperProvider;

public class WorkflowManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowManager.class);

    private final TypeReference<List<TaskDef>> listOfTaskDefs = new TypeReference<>() {
    };

    private Map<String, CompletableFuture<Workflow>> runningWorkflowFutures = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapperProvider().getObjectMapper();

    private final TaskClient taskClient;

    private final WorkflowClient workflowClient;

    private final MetadataClient metadataClient;

    private final ScheduledExecutorService scheduledWorkflowMonitor = Executors.newSingleThreadScheduledExecutor();

    static {
        initTaskImplementations();
    }

    public static void initTaskImplementations() {
        TaskRegistry.register(TaskType.DO_WHILE.name(), DoWhile.class);
        TaskRegistry.register(TaskType.DYNAMIC.name(), Dynamic.class);
        TaskRegistry.register(TaskType.FORK_JOIN_DYNAMIC.name(), DynamicFork.class);
        TaskRegistry.register(TaskType.FORK_JOIN.name(), ForkJoin.class);
        TaskRegistry.register(TaskType.HTTP.name(), Http.class);
        TaskRegistry.register(TaskType.INLINE.name(), Javascript.class);
        TaskRegistry.register(TaskType.JOIN.name(), Join.class);
        TaskRegistry.register(TaskType.JSON_JQ_TRANSFORM.name(), JQ.class);
        TaskRegistry.register(TaskType.SET_VARIABLE.name(), SetVariable.class);
        TaskRegistry.register(TaskType.CUSTOM.name(), CustomTask.class);
        TaskRegistry.register(TaskType.SUB_WORKFLOW.name(), SubWorkflow.class);
        TaskRegistry.register(TaskType.SWITCH.name(), Switch.class);
        TaskRegistry.register(TaskType.TERMINATE.name(), Terminate.class);
        TaskRegistry.register(TaskType.WAIT.name(), Wait.class);
        TaskRegistry.register(TaskType.EVENT.name(), Event.class);
    }

    public WorkflowManager(String apiServerURL) {
        this(apiServerURL, new ClientFilter[0]);
    }

    // public WorkflowManager (String apiServerURL, int pollingInterval,
    // ClientFilter... clientFilter) {
    public WorkflowManager(String apiServerURL, ClientFilter... clientFilter) {

        taskClient = new TaskClient(new DefaultClientConfig(), (ClientHandler) null, clientFilter);
        taskClient.setRootURI(apiServerURL);

        workflowClient = new WorkflowClient(new DefaultClientConfig(), (ClientHandler) null, clientFilter);
        workflowClient.setRootURI(apiServerURL);

        metadataClient = new MetadataClient(new DefaultClientConfig(), (ClientHandler) null, clientFilter);
        metadataClient.setRootURI(apiServerURL);

        // annotatedWorkerHost = new AnnotatedWorkerHost(taskClient, pollingInterval);
        scheduledWorkflowMonitor.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, CompletableFuture<Workflow>> entry : runningWorkflowFutures.entrySet()) {
                String workflowId = entry.getKey();
                CompletableFuture<Workflow> future = entry.getValue();
                Workflow workflow = workflowClient.getWorkflow(workflowId, true);
                if (workflow.getStatus().isTerminal()) {
                    future.complete(workflow);
                    runningWorkflowFutures.remove(workflowId);
                }
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    // public WorkflowManager (TaskClient taskClient, WorkflowClient workflowClient,
    // MetadataClient metadataClient, int pollingInterval) {
    public WorkflowManager(TaskClient taskClient, WorkflowClient workflowClient, MetadataClient metadataClient) {
        this.taskClient = taskClient;
        this.workflowClient = workflowClient;
        this.metadataClient = metadataClient;
        // annotatedWorkerHost = new AnnotatedWorkerHost(taskClient, pollingInterval);
        scheduledWorkflowMonitor.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, CompletableFuture<Workflow>> entry : runningWorkflowFutures.entrySet()) {
                String workflowId = entry.getKey();
                CompletableFuture<Workflow> future = entry.getValue();
                Workflow workflow = workflowClient.getWorkflow(workflowId, true);
                if (workflow.getStatus().isTerminal()) {
                    future.complete(workflow);
                    runningWorkflowFutures.remove(workflowId);
                }
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    // public void initWorkers(String packagesToScan) {
    // annotatedWorkerHost.initWorkers(packagesToScan);
    // }

    public CompletableFuture<Workflow> startWorkflow(String name, Integer version, Object input) {
        CompletableFuture<Workflow> future = new CompletableFuture<>();
        Map<String, Object> inputMap = objectMapper.convertValue(input, Map.class);

        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setInput(inputMap);
        request.setName(name);
        request.setVersion(version);

        String workflowId = workflowClient.startWorkflow(request);
        runningWorkflowFutures.put(workflowId, future);
        return future;
    }

    public CompletableFuture<Workflow> startWorkflow(WorkflowWithInput conductorWorkflow, Object input) {

        CompletableFuture<Workflow> future = new CompletableFuture<>();

        Map<String, Object> inputMap = objectMapper.convertValue(input, Map.class);

        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setInput(inputMap);
        request.setName(conductorWorkflow.getName());
        request.setVersion(conductorWorkflow.getVersion());
        request.setWorkflowDef(conductorWorkflow.toWorkflowDef());

        String workflowId = workflowClient.startWorkflow(request);
        runningWorkflowFutures.put(workflowId, future);

        return future;
    }

    public void loadTaskDefs(String resourcePath) throws IOException {
        InputStream resource = WorkflowManager.class.getResourceAsStream(resourcePath);
        if (resource != null) {
            List<TaskDef> taskDefs = objectMapper.readValue(resource, listOfTaskDefs);
            loadMetadata(taskDefs);
        }
    }

    public void loadWorkflowDef(String resourcePath, boolean overwrite) throws IOException {
        InputStream resource = WorkflowManager.class.getResourceAsStream(resourcePath);
        if (resource != null) {
            WorkflowDef workflowDef = objectMapper.readValue(resource, WorkflowDef.class);
            loadMetadata(workflowDef, overwrite);
        }
    }

    public void loadMetadata(WorkflowDef workflowDef, boolean overwrite) {
        if (overwrite) {
            metadataClient.updateWorkflowDefs(Arrays.asList(workflowDef));
        } else {
            metadataClient.registerWorkflowDef(workflowDef);
        }
    }

    public void loadMetadata(List<TaskDef> taskDefs) {
        metadataClient.registerTaskDefs(taskDefs);
    }

    public void shutdown() {
        scheduledWorkflowMonitor.shutdown();
        // annotatedWorkerHost.shutdown();
    }

    public boolean registerWorkflow(WorkflowDef workflowDef, boolean overwrite) {
        try {
            if (overwrite) {
                metadataClient.updateWorkflowDefs(Arrays.asList(workflowDef));
            } else {
                metadataClient.registerWorkflowDef(workflowDef);
            }

            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

    public MetadataClient getMetadataClient() {
        return metadataClient;
    }

    public TaskClient getTaskClient() {
        return taskClient;
    }

    public WorkflowClient getWorkflowClient() {
        return workflowClient;
    }
}
