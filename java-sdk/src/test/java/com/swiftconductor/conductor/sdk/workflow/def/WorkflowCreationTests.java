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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.swiftconductor.conductor.common.metadata.tasks.TaskType;
import com.swiftconductor.conductor.common.metadata.workflow.WorkflowDef;
import com.swiftconductor.conductor.common.run.Workflow;
import com.swiftconductor.conductor.sdk.testing.TestWorkflowInput;
import com.swiftconductor.conductor.sdk.testing.WorkflowTestRunner;
import com.swiftconductor.conductor.sdk.worker.InputParam;
import com.swiftconductor.conductor.sdk.worker.OutputParam;
import com.swiftconductor.conductor.sdk.worker.Worker;
import com.swiftconductor.conductor.sdk.workflow.WorkflowManager;
import com.swiftconductor.conductor.sdk.workflow.def.tasks.*;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class WorkflowCreationTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowCreationTests.class);

    private static WorkflowManager manager;

    private static WorkflowTestRunner runner;

    @BeforeAll
    public static void init() throws IOException {
        runner = new WorkflowTestRunner(8080, "3.16.0-SNAPSHOT");
        runner.init("com.swiftconductor.conductor.sdk");
        manager = runner.getWorkflowManager();
    }

    @AfterAll
    public static void cleanUp() {
        runner.shutdown();
    }

    @Worker("get_user_info")
    public @OutputParam("zipCode") String getZipCode(@InputParam("name") String userName) {
        return "95014";
    }

    @Worker("task2")
    public @OutputParam("greetings") String task2() {
        return "Hello World";
    }

    @Worker("task3")
    public @OutputParam("greetings") String task3() {
        return "Hello World-3";
    }

    @Worker("fork_gen")
    public DynamicForkInput generateDynamicFork() {
        DynamicForkInput forks = new DynamicForkInput();
        Map<String, Object> inputs = new HashMap<>();
        forks.setInputs(inputs);
        List<Task<?>> tasks = new ArrayList<>();
        forks.setTasks(tasks);

        for (int i = 0; i < 3; i++) {
            CustomTask task = new CustomTask("task2", "fork_task_" + i);
            tasks.add(task);
            HashMap<String, Object> taskInput = new HashMap<>();
            taskInput.put("key", "value");
            taskInput.put("key2", 101);
            inputs.put(task.getTaskReferenceName(), taskInput);
        }
        return forks;
    }

    private WorkflowWithInput<TestWorkflowInput> registerTestWorkflow() throws InterruptedException {
        InputStream script = getClass().getResourceAsStream("/script.js");
        CustomTask getUserInfo = new CustomTask("get_user_info", "get_user_info");
        getUserInfo.input("name", WorkflowWithInput.input.get("name"));

        CustomTask sendToCupertino = new CustomTask("task2", "cupertino");
        CustomTask sendToNYC = new CustomTask("task2", "nyc");

        int len = 4;
        Task<?>[][] parallelTasks = new Task[len][1];
        for (int i = 0; i < len; i++) {
            parallelTasks[i][0] = new CustomTask("task2", "task_parallel_" + i);
        }

        WorkflowBuilder<TestWorkflowInput> builder = new WorkflowBuilder<>();
        TestWorkflowInput defaultInput = new TestWorkflowInput();
        defaultInput.setName("defaultName");

        builder.name("sdk_workflow_example").version(1).ownerEmail("hello@example.com").description("Example Workflow")
                .restartable(true).variables(new WorkflowState())
                .timeoutPolicy(WorkflowDef.TimeoutPolicy.TIME_OUT_WF, 100).defaultInput(defaultInput)
                .add(new Javascript("js", script)).add(new ForkJoin("parallel", parallelTasks)).add(getUserInfo)
                .add(new Switch("decide2", "${workflow.input.zipCode}").switchCase("95014", sendToCupertino)
                        .switchCase("10121", sendToNYC))
                // .add(new SubWorkflow("subflow", "sub_workflow_example", 5))
                .add(new CustomTask("task2", "task222"))
                .add(new DynamicFork("dynamic_fork", new CustomTask("fork_gen", "fork_gen")));

        WorkflowWithInput<TestWorkflowInput> workflow = builder.build();

        var workflowDef = workflow.toWorkflowDef();
        boolean registered = manager.registerWorkflow(workflowDef, true);
        assertTrue(registered);

        return workflow;
    }

    @Test
    public void verifyCreatedWorkflow() throws Exception {
        WorkflowWithInput<TestWorkflowInput> conductorWorkflow = registerTestWorkflow();
        WorkflowDef def = conductorWorkflow.toWorkflowDef();
        assertNotNull(def);
        assertTrue(
                def.getTasks().get(def.getTasks().size() - 2).getType().equals(TaskType.TASK_TYPE_FORK_JOIN_DYNAMIC));
        assertTrue(def.getTasks().get(def.getTasks().size() - 1).getType().equals(TaskType.TASK_TYPE_JOIN));
    }

    @Test
    public void verifyInlineWorkflowExecution() throws ValidationError {
        TestWorkflowInput workflowInput = new TestWorkflowInput("username", "10121", "US");
        try {
            var workflow = registerTestWorkflow();
            Workflow run = manager.startWorkflow(workflow.getName(), workflow.getVersion(), workflowInput).get(10, TimeUnit.SECONDS);
            assertEquals(Workflow.WorkflowStatus.COMPLETED, run.getStatus(), run.getReasonForIncompletion());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testWorkflowExecutionByName() throws ExecutionException, InterruptedException {

        // Register the workflow first
        registerTestWorkflow();

        WorkflowDef def = manager.getMetadataClient().getWorkflowDef("sdk_workflow_example", null);
        WorkflowWithInput<TestWorkflowInput> conductorWorkflow = WorkflowWithInput.fromWorkflowDef(def);

        TestWorkflowInput input = new TestWorkflowInput("username", "10121", "US");
        CompletableFuture<Workflow> run = manager.startWorkflow(conductorWorkflow, input);

        try {
            run.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void verifyWorkflowExecutionFailsIfNotExists() throws ExecutionException, InterruptedException {

        // Register the workflow first
        registerTestWorkflow();

        try {
            WorkflowDef def = manager.getMetadataClient().getWorkflowDef("non_existent_workflow", null);
            WorkflowWithInput<TestWorkflowInput> conductorWorkflow = WorkflowWithInput.fromWorkflowDef(def);

            TestWorkflowInput input = new TestWorkflowInput("username", "10121", "US");
            manager.startWorkflow(conductorWorkflow, input);

            fail("execution should have failed");
        } catch (Exception e) {
        }
    }
}
