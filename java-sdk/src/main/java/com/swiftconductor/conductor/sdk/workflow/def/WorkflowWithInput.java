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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.swiftconductor.conductor.common.metadata.workflow.WorkflowDef;
import com.swiftconductor.conductor.common.metadata.workflow.WorkflowTask;
import com.swiftconductor.conductor.sdk.workflow.def.tasks.Task;
import com.swiftconductor.conductor.sdk.workflow.def.tasks.TaskRegistry;
import com.swiftconductor.conductor.sdk.workflow.utils.InputOutputGetter;
import com.swiftconductor.conductor.sdk.workflow.utils.ObjectMapperProvider;

/**
 * @param <T>
 *            Type of the workflow input
 */
public class WorkflowWithInput<T> {

    public static final InputOutputGetter input = new InputOutputGetter("workflow", InputOutputGetter.Field.input);

    public static final InputOutputGetter output = new InputOutputGetter("workflow", InputOutputGetter.Field.output);

    private String name;

    private String description;

    private int version;

    private String failureWorkflow;

    private String ownerEmail;

    private WorkflowDef.TimeoutPolicy timeoutPolicy;

    private Map<String, Object> workflowOutput;

    private long timeoutSeconds;

    private boolean restartable = true;

    private T defaultInput;

    private Map<String, Object> variables;

    private List<Task> tasks = new ArrayList<>();

    private final ObjectMapper objectMapper = new ObjectMapperProvider().getObjectMapper();

    public WorkflowWithInput() {
        this.workflowOutput = new HashMap<>();
        this.restartable = true;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setFailureWorkflow(String failureWorkflow) {
        this.failureWorkflow = failureWorkflow;
    }

    public void add(Task task) {
        this.tasks.add(task);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public int getVersion() {
        return version;
    }

    public String getFailureWorkflow() {
        return failureWorkflow;
    }

    public String getOwnerEmail() {
        return ownerEmail;
    }

    public void setOwnerEmail(String ownerEmail) {
        this.ownerEmail = ownerEmail;
    }

    public WorkflowDef.TimeoutPolicy getTimeoutPolicy() {
        return timeoutPolicy;
    }

    public void setTimeoutPolicy(WorkflowDef.TimeoutPolicy timeoutPolicy) {
        this.timeoutPolicy = timeoutPolicy;
    }

    public long getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public boolean isRestartable() {
        return restartable;
    }

    public void setRestartable(boolean restartable) {
        this.restartable = restartable;
    }

    public T getDefaultInput() {
        return defaultInput;
    }

    public void setDefaultInput(T defaultInput) {
        this.defaultInput = defaultInput;
    }

    public Map<String, Object> getWorkflowOutput() {
        return workflowOutput;
    }

    public void setWorkflowOutput(Map<String, Object> workflowOutput) {
        this.workflowOutput = workflowOutput;
    }

    public Object getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }

    /**
     * @return Convert to the WorkflowDef model used by the Metadata APIs
     */
    public WorkflowDef toWorkflowDef() {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setDescription(description);
        def.setVersion(version);
        def.setFailureWorkflow(failureWorkflow);
        def.setOwnerEmail(ownerEmail);
        def.setTimeoutPolicy(timeoutPolicy);
        def.setTimeoutSeconds(timeoutSeconds);
        def.setRestartable(restartable);
        def.setOutputParameters(workflowOutput);
        def.setVariables(variables);
        def.setInputTemplate(objectMapper.convertValue(defaultInput, Map.class));

        for (Task task : tasks) {
            def.getTasks().addAll(task.getWorkflowDefTasks());
        }
        return def;
    }

    /**
     * Generate WorkflowWithInput based on the workflow metadata definition
     *
     * @param def
     * @return
     */
    public static <T> WorkflowWithInput<T> fromWorkflowDef(WorkflowDef def) {
        WorkflowWithInput<T> workflow = new WorkflowWithInput<>();
        fromWorkflowDef(workflow, def);
        return workflow;
    }

    private static <T> void fromWorkflowDef(WorkflowWithInput<T> workflow, WorkflowDef def) {
        workflow.setName(def.getName());
        workflow.setVersion(def.getVersion());
        workflow.setFailureWorkflow(def.getFailureWorkflow());
        workflow.setRestartable(def.isRestartable());
        workflow.setVariables(def.getVariables());
        workflow.setDefaultInput((T) def.getInputTemplate());

        workflow.setWorkflowOutput(def.getOutputParameters());
        workflow.setOwnerEmail(def.getOwnerEmail());
        workflow.setDescription(def.getDescription());
        workflow.setTimeoutSeconds(def.getTimeoutSeconds());
        workflow.setTimeoutPolicy(def.getTimeoutPolicy());

        List<WorkflowTask> workflowTasks = def.getTasks();
        for (WorkflowTask workflowTask : workflowTasks) {
            Task task = TaskRegistry.getTask(workflowTask);
            workflow.tasks.add(task);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        WorkflowWithInput workflow = (WorkflowWithInput) o;
        return version == workflow.version && Objects.equals(name, workflow.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version);
    }

    @Override
    public String toString() {
        try {
            return objectMapper.writeValueAsString(toWorkflowDef());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
