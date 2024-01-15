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
package com.swiftconductor.conductor.sdk.testing;

import com.swiftconductor.conductor.client.http.TaskClient;
import com.swiftconductor.conductor.sdk.worker.AnnotatedWorkerHost;
import com.swiftconductor.conductor.sdk.workflow.WorkflowManager;

public class WorkflowTestRunner {

    private LocalServerRunner localServerRunner;

    private final AnnotatedWorkerHost annotatedWorkerHost;

    private final WorkflowManager workflowManager;

    public WorkflowTestRunner(String serverApiUrl) {

        TaskClient taskClient = new TaskClient();
        taskClient.setRootURI(serverApiUrl);
        this.annotatedWorkerHost = new AnnotatedWorkerHost(taskClient);

        this.workflowManager = new WorkflowManager(serverApiUrl);
    }

    public WorkflowTestRunner(int port, String conductorVersion) {

        localServerRunner = new LocalServerRunner(port, conductorVersion);

        TaskClient taskClient = new TaskClient();
        taskClient.setRootURI(localServerRunner.getServerAPIUrl());
        this.annotatedWorkerHost = new AnnotatedWorkerHost(taskClient);

        this.workflowManager = new WorkflowManager(localServerRunner.getServerAPIUrl());
    }

    public WorkflowManager getWorkflowManager() {
        return workflowManager;
    }

    public void init(String basePackages) {
        if (localServerRunner != null) {
            localServerRunner.startLocalServer();
        }
        annotatedWorkerHost.initWorkers(basePackages);
    }

    public void shutdown() {
        localServerRunner.shutdown();
        annotatedWorkerHost.shutdown();
        workflowManager.shutdown();
    }
}
