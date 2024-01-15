# Workflows

## APIs

### ConductorWorkflow

[ConductorWorkflow](https://github.com/conductor-client-java/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/ConductorWorkflow.java) is the SDK representation of a Conductor workflow.

#### Create a `ConductorWorkflow` Instance

```java
ConductorWorkflow<GetInsuranceQuote> conductorWorkflow = new WorkflowBuilder<GetInsuranceQuote>()
    .name("sdk_workflow_example")
    .version(1)
    .ownerEmail("hello@example.com")
    .description("Example Workflow")
    .timeoutPolicy(WorkflowDef.TimeoutPolicy.TIME_OUT_WF, 100)
    .add(new CustomTask("calculate_insurance_premium", "calculate_insurance_premium"))
    .add(new CustomTask("send_email", "send_email"))
    .build();
```

### Working with CUSTOM Tasks

Use [CustomTask](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/CustomTask.java) to add a custom tasks to a workflow.

Example:

```java
...
builder.add(new CustomTask("send_email", "send_email"))
...
```
### Wiring Inputs to Task

Use `input` methods to configure the inputs to the task.

See our doc on [task inputs](https://swiftconductor.com/how-tos/Tasks/task-inputs.html) for more details.

Example

```java
builder.add(
        new CustomTask("send_email", "send_email")
                .input("email", "${workflow.input.email}")
                .input("subject", "Your insurance quote for the amount ${generate_quote.output.amount}")
);
```

### Working with Operators

Each operator has its own class that can be added to the workflow builder.

* [ForkJoin](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/ForkJoin.java) 
* [Wait](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/Wait.java)
* [Switch](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/Switch.java)
* [DynamicFork](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/DynamicFork.java)
* [DoWhile](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/DoWhile.java)
* [Join](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/Join.java)
* [Dynamic](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/Dynamic.java)
* [Terminate](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/Terminate.java)
* [SubWorkflow](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/SubWorkflow.java)
* [SetVariable](https://github.com/swift-conductor/conductor/blob/main/java-sdk/src/main/java/com/swiftconductor/conductor/sdk/workflow/def/tasks/SetVariable.java)


#### Register Workflow with Conductor Server

```java

// Returns true if the workflow is successfully created
// Reasons why this method will return false
// 1. Network connectivity issue
// 2. Workflow already exists with the specified name and version 
// 3. There are missing task definitions

var workflowDef = conductorWorkflow.toWorkflowDef()
boolean registered = manager.registerWorkflow(workflowDef, false);
```

#### Overwrite Existing Workflow Definition​

```java
boolean registered = manager.registerWorkflow(workflowDef, true);
```

#### Create `ConductorWorkflow` based on the definition registered on the server

```java
WorkflowDef workflowDef = manager.getMetadataClient().getWorkflowDef("sdk_workflow_example", 1);
ConductorWorkflow<GetInsuranceQuote> conductorWorkflow = ConductorWorkflow.fromWorkflowDef(workflowDef);
```

#### Start Workflow Execution

Start the execution of the workflow based on the definition registered on the server. Use the register method to register a workflow on the server before executing.

```java

// Returns a completable future
CompletableFuture<Workflow> run = manager.startWorkflow(workflowDef.getName(), workflowDef.getVersion(), input);

// Wait for the workflow to complete -- useful if workflow completes within a reasonable amount of time
Workflow workflowRun = run.get();

// Get the workflowId
String workflowId = workflowRun.getWorkflowId();

// Get the status of workflow execution
WorkflowStatus status = workflowRun.getStatus();
```

See [Workflow](https://github.com/swift-conductor/conductor/blob/main/common/src/main/java/com/swiftconductor/conductor/common/run/Workflow.java) for more details on the Workflow object.

#### Start Dynamic Workflow Execution

Dynamic workflows are executed by specifying the workflow definition along with the execution and do not require registering the workflow on the server before executing.

##### Use cases for dynamic workflows

1. Each workflow run has a unique workflow definition 
2. Workflows are defined based on the user data and cannot be modeled ahead of time statically 

```java
// 1. Use WorkflowBuilder to create ConductorWorkflow.
ConductorWorkflow<GetInsuranceQuote> conductorWorkflow = new WorkflowBuilder<GetInsuranceQuote>()
    .name("sdk_workflow_example")
    .version(1)
    .ownerEmail("hello@example.com")
    .description("Example Workflow")
    .timeoutPolicy(WorkflowDef.TimeoutPolicy.TIME_OUT_WF, 100)
    .add(new CustomTask("calculate_insurance_premium", "calculate_insurance_premium"))
    .add(new CustomTask("send_email", "send_email"))
    .build();

// 2. Start using the definition created by SDK.
TestWorkflowInput input = new GetInsuranceQuoteInput("username", "10121", "US");
CompletableFuture<Workflow> run = manager.startWorkflow(conductorWorkflow, input);

try {
    run.get(10, TimeUnit.SECONDS);
} catch (Exception e) {
    e.printStackTrace();
    fail(e.getMessage());
}

```






