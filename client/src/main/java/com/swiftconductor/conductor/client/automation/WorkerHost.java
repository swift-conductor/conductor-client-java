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
package com.swiftconductor.conductor.client.automation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.discovery.EurekaClient;

import com.swiftconductor.conductor.client.exception.ClientException;
import com.swiftconductor.conductor.client.http.TaskClient;
import com.swiftconductor.conductor.client.worker.AbstractWorker;

/**
 * Configures automated polling of tasks and execution via the registered
 * {@link AbstractWorker}s.
 */
public class WorkerHost {
    static final Logger LOGGER = LoggerFactory.getLogger(WorkerHost.class);
    private static final String INVALID_THREAD_COUNT = "Invalid worker thread count specified, use either shared thread pool or config thread count per task";

    private ScheduledExecutorService scheduledExecutorService;

    private final EurekaClient eurekaClient;
    private final TaskClient taskClient;
    private final List<AbstractWorker> workers = new LinkedList<>();
    private final int sleepWhenRetry;
    private final int updateRetryCount;
    @Deprecated
    private final int threadCount;
    private final int shutdownGracePeriodSeconds;
    private final String workerNamePrefix;
    private final Map<String /* taskType */, String /* domain */> taskToDomain;
    private final Map<String /* taskType */, Integer /* threadCount */> taskThreadCount;

    private WorkerProcess taskPollExecutor;

    /**
     * @see WorkerHost.Builder
     * @see WorkerHost#init()
     */
    private WorkerHost(Builder builder) {
        // WorkerHosthared thread pool or per task thread pool
        if (builder.threadCount != -1 && !builder.taskThreadCount.isEmpty()) {
            LOGGER.error(INVALID_THREAD_COUNT);
            throw new ClientException(INVALID_THREAD_COUNT);
        } else if (!builder.taskThreadCount.isEmpty()) {
            for (AbstractWorker worker : builder.workers) {
                if (!builder.taskThreadCount.containsKey(worker.getTaskDefName())) {
                    LOGGER.info("No thread count specified for task type {}, default to 1 thread", worker.getTaskDefName());
                    builder.taskThreadCount.put(worker.getTaskDefName(), 1);
                }
                workers.add(worker);
            }
            this.taskThreadCount = builder.taskThreadCount;
            this.threadCount = -1;
        } else {
            Set<String> taskTypes = new HashSet<>();
            for (AbstractWorker worker : builder.workers) {
                taskTypes.add(worker.getTaskDefName());
                workers.add(worker);
            }

            this.threadCount = (builder.threadCount == -1) ? workers.size() : builder.threadCount;

            // shared thread pool will be evenly split between task types
            int splitThreadCount = threadCount / taskTypes.size();
            this.taskThreadCount = taskTypes.stream().collect(Collectors.toMap(v -> v, v -> splitThreadCount));
        }

        this.eurekaClient = builder.eurekaClient;
        this.taskClient = builder.taskClient;
        this.sleepWhenRetry = builder.sleepWhenRetry;
        this.updateRetryCount = builder.updateRetryCount;
        this.workerNamePrefix = builder.workerNamePrefix;
        this.taskToDomain = builder.taskToDomain;
        this.shutdownGracePeriodSeconds = builder.shutdownGracePeriodSeconds;
    }

    /** Builder used to create the instances of WorkerHost */
    public static class Builder {
        private String workerNamePrefix = "workflow-worker-%d";
        private int sleepWhenRetry = 500;
        private int updateRetryCount = 3;

        @Deprecated
        private int threadCount = -1;

        private int shutdownGracePeriodSeconds = 10;
        private final Iterable<AbstractWorker> workers;
        private EurekaClient eurekaClient;
        private final TaskClient taskClient;
        private Map<String /* taskType */, String /* domain */> taskToDomain = new HashMap<>();
        private Map<String /* taskType */, Integer /* threadCount */> taskThreadCount = new HashMap<>();

        public Builder(TaskClient taskClient, Iterable<AbstractWorker> workers) {
            Validate.notNull(taskClient, "TaskClient cannot be null");
            Validate.notNull(workers, "Workers cannot be null");
            this.taskClient = taskClient;
            this.workers = workers;
        }

        /**
         * @param workerNamePrefix
         *            prefix to be used for worker names, defaults to workflow-worker-
         *            if not supplied.
         * @return Returns the current instance.
         */
        public Builder withWorkerNamePrefix(String workerNamePrefix) {
            this.workerNamePrefix = workerNamePrefix;
            return this;
        }

        /**
         * @param sleepWhenRetry
         *            time in milliseconds, for which the thread should sleep when task
         *            update call fails, before retrying the operation.
         * @return Returns the current instance.
         */
        public Builder withSleepWhenRetry(int sleepWhenRetry) {
            this.sleepWhenRetry = sleepWhenRetry;
            return this;
        }

        /**
         * @param updateRetryCount
         *            number of times to retry the failed updateTask operation
         * @return Builder instance
         * @see #withSleepWhenRetry(int)
         */
        public Builder withUpdateRetryCount(int updateRetryCount) {
            this.updateRetryCount = updateRetryCount;
            return this;
        }

        /**
         * @param threadCount
         *            # of threads assigned to the workers. Should be at-least the size
         *            of taskWorkers to avoid starvation in a busy system.
         * @return Builder instance
         * @deprecated Use {@link WorkerHost.Builder#withTaskThreadCount(Map)} instead.
         */
        @Deprecated
        public Builder withThreadCount(int threadCount) {
            if (threadCount < 1) {
                throw new IllegalArgumentException("No. of threads cannot be less than 1");
            }
            this.threadCount = threadCount;
            return this;
        }

        /**
         * @param shutdownGracePeriodSeconds
         *            waiting seconds before forcing shutdown of your worker
         * @return Builder instance
         */
        public Builder withShutdownGracePeriodSeconds(int shutdownGracePeriodSeconds) {
            if (shutdownGracePeriodSeconds < 1) {
                throw new IllegalArgumentException("Seconds of shutdownGracePeriod cannot be less than 1");
            }
            this.shutdownGracePeriodSeconds = shutdownGracePeriodSeconds;
            return this;
        }

        /**
         * @param eurekaClient
         *            Eureka client - used to identify if the server is in discovery or
         *            not. When the server goes out of discovery, the polling is
         *            terminated. If passed null, discovery check is not done.
         * @return Builder instance
         */
        public Builder withEurekaClient(EurekaClient eurekaClient) {
            this.eurekaClient = eurekaClient;
            return this;
        }

        public Builder withTaskToDomain(Map<String, String> taskToDomain) {
            this.taskToDomain = taskToDomain;
            return this;
        }

        public Builder withTaskThreadCount(Map<String, Integer> taskThreadCount) {
            this.taskThreadCount = taskThreadCount;
            return this;
        }

        /**
         * Builds an instance of the WorkerHost.
         *
         * <p>
         * Please see {@link WorkerHost#init()} method. The method must be called after
         * this constructor for the polling to start.
         */
        public WorkerHost build() {
            return new WorkerHost(this);
        }
    }

    /**
     * @return Thread Count for the shared executor pool
     */
    @Deprecated
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * @return Thread Count for individual task type
     */
    public Map<String, Integer> getTaskThreadCount() {
        return taskThreadCount;
    }

    /**
     * @return seconds before forcing shutdown of worker
     */
    public int getShutdownGracePeriodSeconds() {
        return shutdownGracePeriodSeconds;
    }

    /**
     * @return sleep time in millisecond before task update retry is done when
     *         receiving error from the Conductor server
     */
    public int getSleepWhenRetry() {
        return sleepWhenRetry;
    }

    /**
     * @return Number of times updateTask should be retried when receiving error
     *         from Conductor server
     */
    public int getUpdateRetryCount() {
        return updateRetryCount;
    }

    /**
     * @return prefix used for worker names
     */
    public String getWorkerNamePrefix() {
        return workerNamePrefix;
    }

    /**
     * Starts the polling. Must be called after {@link WorkerHost.Builder#build()}
     * method.
     */
    public synchronized void init() {
        this.taskPollExecutor = new WorkerProcess(
                eurekaClient, taskClient, updateRetryCount,
                taskToDomain, workerNamePrefix, taskThreadCount);

        this.scheduledExecutorService = Executors.newScheduledThreadPool(workers.size());
        workers.forEach(worker -> scheduledExecutorService.scheduleWithFixedDelay(() -> taskPollExecutor.pollAndExecute(worker),
                worker.getPollingInterval(),
                worker.getPollingInterval(),
                TimeUnit.MILLISECONDS));
    }

    /**
     * Invoke this method within a PreDestroy block within your application to
     * facilitate a graceful shutdown of your worker, during process termination.
     */
    public void shutdown() {
        taskPollExecutor.shutdownAndAwaitTermination(scheduledExecutorService, shutdownGracePeriodSeconds);
        taskPollExecutor.shutdown(shutdownGracePeriodSeconds);
    }
}
