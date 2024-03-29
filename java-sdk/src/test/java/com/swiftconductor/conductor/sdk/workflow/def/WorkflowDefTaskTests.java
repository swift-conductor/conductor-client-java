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

import org.junit.jupiter.api.Test;

import com.swiftconductor.conductor.common.metadata.workflow.WorkflowTask;
import com.swiftconductor.conductor.sdk.workflow.WorkflowManager;
import com.swiftconductor.conductor.sdk.workflow.def.tasks.*;

import static org.junit.jupiter.api.Assertions.*;

public class WorkflowDefTaskTests {

    static {
        WorkflowManager.initTaskImplementations();
    }

    @Test
    public void testWorkflowDefTaskWithStartDelay() {
        CustomTask customTask = new CustomTask("task_name", "task_ref_name");
        int startDelay = 5;

        customTask.setStartDelay(startDelay);

        WorkflowTask workflowTask = customTask.getWorkflowDefTasks().get(0);

        assertEquals(customTask.getStartDelay(), workflowTask.getStartDelay());
        assertEquals(startDelay, customTask.getStartDelay());
        assertEquals(startDelay, workflowTask.getStartDelay());
    }

    @Test
    public void testWorkflowDefTaskWithOptionalEnabled() {
        CustomTask customTask = new CustomTask("task_name", "task_ref_name");

        customTask.setOptional(true);

        WorkflowTask workflowTask = customTask.getWorkflowDefTasks().get(0);

        assertEquals(customTask.getStartDelay(), workflowTask.getStartDelay());
        assertEquals(true, customTask.isOptional());
        assertEquals(true, workflowTask.isOptional());
    }
}
