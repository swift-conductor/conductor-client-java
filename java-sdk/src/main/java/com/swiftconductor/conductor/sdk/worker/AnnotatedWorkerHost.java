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
package com.swiftconductor.conductor.sdk.worker;

import java.lang.reflect.Method;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.reflect.ClassPath;
import com.swiftconductor.conductor.client.automation.WorkerHost;
import com.swiftconductor.conductor.client.http.TaskClient;
import com.swiftconductor.conductor.client.worker.AbstractWorker;

public class AnnotatedWorkerHost {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnnotatedWorkerHost.class);

    private TaskClient taskClient;

    private WorkerHost workerHost;

    private List<AbstractWorker> workers = new ArrayList<>();

    private Map<String, Method> workerProcesses = new HashMap<>();

    private Map<String, Integer> workerToThreadCount = new HashMap<>();

    private Map<String, Integer> workerToPollingInterval = new HashMap<>();

    private Map<String, String> workerDomains = new HashMap<>();

    private Map<String, Object> workerClassObjs = new HashMap<>();

    private static Set<String> scannedPackages = new HashSet<>();

    private WorkerConfiguration workerConfiguration;

    public AnnotatedWorkerHost(TaskClient taskClient) {
        this.taskClient = taskClient;
        this.workerConfiguration = new WorkerConfiguration();
    }

    public AnnotatedWorkerHost(TaskClient taskClient, int pollingIntervalInMillis) {
        this.taskClient = taskClient;
        this.workerConfiguration = new WorkerConfiguration(pollingIntervalInMillis);
    }

    public AnnotatedWorkerHost(TaskClient taskClient, WorkerConfiguration workerConfiguration) {
        this.taskClient = taskClient;
        this.workerConfiguration = workerConfiguration;
    }

    /**
     * Finds any worker implementation and starts polling for tasks
     *
     * @param basePackage
     *            list of packages - comma separated - to scan for annotated worker
     *            implementation
     */
    public synchronized void initWorkers(String basePackage) {
        scanWorkers(basePackage);
        startPolling();
    }

    /** Shuts down the workers */
    public void shutdown() {
        if (workerHost != null) {
            workerHost.shutdown();
        }
    }

    private void scanWorkers(String basePackage) {
        try {
            if (scannedPackages.contains(basePackage)) {
                // skip
                LOGGER.info("Package {} already scanned and will skip", basePackage);
                return;
            }

            // Add here so to avoid infinite recursion where a class in the package contains
            // the
            // code to init workers
            scannedPackages.add(basePackage);
            List<String> packagesToScan = new ArrayList<>();
            if (basePackage != null) {
                String[] packages = basePackage.split(",");
                Collections.addAll(packagesToScan, packages);
            }

            LOGGER.info("packages to scan {}", packagesToScan);

            long s = System.currentTimeMillis();
            ClassPath.from(AnnotatedWorkerHost.class.getClassLoader()).getAllClasses().forEach(classMeta -> {
                String name = classMeta.getName();
                if (!includePackage(packagesToScan, name)) {
                    return;
                }
                try {
                    Class<?> clazz = classMeta.load();
                    Object obj = clazz.getConstructor().newInstance();
                    addBean(obj);
                } catch (Throwable t) {
                    // trace because many classes won't have a default no-args
                    // constructor and will fail
                    LOGGER.trace("Caught exception while loading and scanning class {}", t.getMessage());
                }
            });

            LOGGER.info("Took {} ms to scan all the classes, loading {} tasks", (System.currentTimeMillis() - s), workerProcesses.size());
        } catch (Exception e) {
            LOGGER.error("Error while scanning for workers: ", e);
        }
    }

    private boolean includePackage(List<String> packagesToScan, String name) {
        for (String scanPkg : packagesToScan) {
            if (name.startsWith(scanPkg))
                return true;
        }

        return false;
    }

    public void addBean(Object bean) {
        Class<?> clazz = bean.getClass();
        for (Method method : clazz.getMethods()) {
            Worker annotation = method.getAnnotation(Worker.class);
            if (annotation == null) {
                continue;
            }
            addMethod(annotation, method, bean);
        }
    }

    private void addMethod(Worker annotation, Method method, Object bean) {
        String name = annotation.value();

        int threadCount = workerConfiguration.getThreadCount(name);
        if (threadCount == 0) {
            threadCount = annotation.threadCount();
        }
        workerToThreadCount.put(name, threadCount);

        int pollingInterval = workerConfiguration.getPollingInterval(name);
        if (pollingInterval == 0) {
            pollingInterval = annotation.pollingInterval();
        }
        workerToPollingInterval.put(name, pollingInterval);

        String domain = workerConfiguration.getDomain(name);
        if (Strings.isNullOrEmpty(domain)) {
            domain = annotation.domain();
        }
        if (!Strings.isNullOrEmpty(domain)) {
            workerDomains.put(name, domain);
        }

        workerClassObjs.put(name, bean);
        workerProcesses.put(name, method);
        LOGGER.info("Adding worker for task {}, method {} with threadCount {} and polling interval set to {} ms", name, method, threadCount, pollingInterval);
    }

    public void startPolling() {
        workerProcesses.forEach((taskName, method) -> {
            Object obj = workerClassObjs.get(taskName);
            AnnotatedWorker worker = new AnnotatedWorker(taskName, method, obj);
            worker.setPollingInterval(workerToPollingInterval.get(taskName));
            workers.add(worker);
        });

        if (workers.isEmpty()) {
            return;
        }

        LOGGER.info("Starting workers with threadCount {}", workerToThreadCount);
        LOGGER.info("Worker domains {}", workerDomains);

        workerHost = new WorkerHost.Builder(taskClient, workers).withTaskThreadCount(workerToThreadCount).withTaskToDomain(workerDomains).build();

        workerHost.init();
    }

    @VisibleForTesting
    List<AbstractWorker> getWorkers() {
        return workers;
    }

    @VisibleForTesting
    WorkerHost getWorkerHost() {
        return workerHost;
    }
}
