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
package com.swiftconductor.conductor.sdk.workflow.def;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swiftconductor.conductor.common.metadata.workflow.WorkflowDef;
import com.swiftconductor.conductor.common.metadata.workflow.WorkflowTask;
import com.swiftconductor.conductor.sdk.workflow.def.tasks.*;
import com.swiftconductor.conductor.sdk.workflow.utils.InputOutputGetter;
import com.swiftconductor.conductor.sdk.workflow.utils.MapBuilder;
import com.swiftconductor.conductor.sdk.workflow.utils.ObjectMapperProvider;

/**
 * @param <T>
 *            Input type for the workflow
 */
public class WorkflowBuilder<T> {

    private String name;

    private String description;

    private int version;

    private String failureWorkflow;

    private String ownerEmail;

    private WorkflowDef.TimeoutPolicy timeoutPolicy;

    private long timeoutSeconds;

    private boolean restartable = true;

    private T defaultInput;

    private Map<String, Object> output = new HashMap<>();

    private Map<String, Object> state;

    protected List<Task<?>> tasks = new ArrayList<>();

    public final InputOutputGetter input = new InputOutputGetter("workflow", InputOutputGetter.Field.input);

    private final ObjectMapper objectMapper = new ObjectMapperProvider().getObjectMapper();

    public WorkflowBuilder() {
        this.tasks = new ArrayList<>();
    }

    public WorkflowBuilder<T> name(String name) {
        this.name = name;
        return this;
    }

    public WorkflowBuilder<T> version(int version) {
        this.version = version;
        return this;
    }

    public WorkflowBuilder<T> description(String description) {
        this.description = description;
        return this;
    }

    public WorkflowBuilder<T> failureWorkflow(String failureWorkflow) {
        this.failureWorkflow = failureWorkflow;
        return this;
    }

    public WorkflowBuilder<T> ownerEmail(String ownerEmail) {
        this.ownerEmail = ownerEmail;
        return this;
    }

    public WorkflowBuilder<T> timeoutPolicy(WorkflowDef.TimeoutPolicy timeoutPolicy, long timeoutSeconds) {
        this.timeoutPolicy = timeoutPolicy;
        this.timeoutSeconds = timeoutSeconds;
        return this;
    }

    public WorkflowBuilder<T> add(Task<?>... tasks) {
        Collections.addAll(this.tasks, tasks);
        return this;
    }

    public WorkflowBuilder<T> defaultInput(T defaultInput) {
        this.defaultInput = defaultInput;
        return this;
    }

    public WorkflowBuilder<T> restartable(boolean restartable) {
        this.restartable = restartable;
        return this;
    }

    public WorkflowBuilder<T> variables(Object variables) {
        try {
            this.state = objectMapper.convertValue(variables, Map.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Workflow Variables cannot be converted to Map.  Supplied: " + variables.getClass().getName());
        }
        return this;
    }

    public WorkflowBuilder<T> output(String key, boolean value) {
        output.put(key, value);
        return this;
    }

    public WorkflowBuilder<T> output(String key, String value) {
        output.put(key, value);
        return this;
    }

    public WorkflowBuilder<T> output(String key, Number value) {
        output.put(key, value);
        return this;
    }

    public WorkflowBuilder<T> output(String key, Object value) {
        output.put(key, value);
        return this;
    }

    public WorkflowBuilder<T> output(MapBuilder mapBuilder) {
        output.putAll(mapBuilder.build());
        return this;
    }

    public WorkflowWithInput<T> build() throws ValidationError {

        validate();

        WorkflowWithInput<T> workflow = new WorkflowWithInput<T>();
        if (description != null) {
            workflow.setDescription(description);
        }

        workflow.setName(name);
        workflow.setVersion(version);
        workflow.setDescription(description);
        workflow.setFailureWorkflow(failureWorkflow);
        workflow.setOwnerEmail(ownerEmail);
        workflow.setTimeoutPolicy(timeoutPolicy);
        workflow.setTimeoutSeconds(timeoutSeconds);
        workflow.setRestartable(restartable);
        workflow.setDefaultInput(defaultInput);
        workflow.setWorkflowOutput(output);
        workflow.setVariables(state);

        for (Task task : tasks) {
            workflow.add(task);
        }

        return workflow;
    }

    /**
     * Validate: 1. There are no tasks with duplicate reference names 2. Each of the
     * task is consistent with its definition 3.
     */
    private void validate() throws ValidationError {

        List<WorkflowTask> allTasks = new ArrayList<>();
        for (Task task : tasks) {
            List<WorkflowTask> workflowDefTasks = task.getWorkflowDefTasks();
            for (WorkflowTask workflowDefTask : workflowDefTasks) {
                allTasks.addAll(workflowDefTask.collectTasks());
            }
        }

        Map<String, WorkflowTask> taskMap = new HashMap<>();
        Set<String> duplicateTasks = new HashSet<>();
        for (WorkflowTask task : allTasks) {
            if (taskMap.containsKey(task.getTaskReferenceName())) {
                duplicateTasks.add(task.getTaskReferenceName());
            } else {
                taskMap.put(task.getTaskReferenceName(), task);
            }
        }
        if (!duplicateTasks.isEmpty()) {
            throw new ValidationError("Task Reference Names MUST be unique across all the tasks in the workkflow.  "
                    + "Please update/change reference names to be unique for the following tasks: " + duplicateTasks);
        }
    }
}
