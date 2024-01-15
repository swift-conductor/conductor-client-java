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
package com.swiftconductor.conductor.sdk.example.shipment;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.swiftconductor.conductor.common.metadata.workflow.WorkflowDef;
import com.swiftconductor.conductor.common.run.Workflow;
import com.swiftconductor.conductor.sdk.worker.AnnotatedWorkerHost;
import com.swiftconductor.conductor.sdk.workflow.WorkflowManager;
import com.swiftconductor.conductor.sdk.workflow.def.WorkflowBuilder;
import com.swiftconductor.conductor.sdk.workflow.def.WorkflowWithInput;
import com.swiftconductor.conductor.sdk.workflow.def.tasks.*;

public class ShipmentWorkflow {

    private final WorkflowManager manager;
    private final AnnotatedWorkerHost annotatedWorkerHost;

    public ShipmentWorkflow(WorkflowManager manager) {
        this.manager = manager;

        this.annotatedWorkerHost = new AnnotatedWorkerHost(manager.getTaskClient());
        this.annotatedWorkerHost.initWorkers(ShipmentWorkflow.class.getPackageName());
    }

    public WorkflowWithInput<Order> createOrderFlow() {
        WorkflowBuilder<Order> builder = new WorkflowBuilder<>();
        builder.name("order_flow").version(1).ownerEmail("user@example.com")
                .timeoutPolicy(WorkflowDef.TimeoutPolicy.TIME_OUT_WF, 60) // 1 day max
                .description("Workflow to track shipment")
                .add(new CustomTask("calculate_tax_and_total", "calculate_tax_and_total").input("orderDetail",
                        WorkflowWithInput.input.get("orderDetail")))
                .add(new CustomTask("charge_payment", "charge_payment").input("billingId",
                        WorkflowWithInput.input.map("userDetails").get("billingId"), "billingType",
                        WorkflowWithInput.input.map("userDetails").get("billingType"), "amount",
                        "${calculate_tax_and_total.output.total_amount}"))
                .add(new Switch("shipping_label", "${workflow.input.orderDetail.shippingMethod}")
                        .switchCase(Order.ShippingMethod.GROUND.toString(),
                                new CustomTask("ground_shipping_label", "ground_shipping_label").input("name",
                                        WorkflowWithInput.input.map("userDetails").get("name"), "address",
                                        WorkflowWithInput.input.map("userDetails").get("addressLine"), "orderNo",
                                        WorkflowWithInput.input.map("orderDetail").get("orderNumber")))
                        .switchCase(Order.ShippingMethod.NEXT_DAY_AIR.toString(),
                                new CustomTask("air_shipping_label", "air_shipping_label").input("name",
                                        WorkflowWithInput.input.map("userDetails").get("name"), "address",
                                        WorkflowWithInput.input.map("userDetails").get("addressLine"), "orderNo",
                                        WorkflowWithInput.input.map("orderDetail").get("orderNumber")))
                        .switchCase(Order.ShippingMethod.SAME_DAY.toString(),
                                new CustomTask("same_day_shipping_label", "same_day_shipping_label").input("name",
                                        WorkflowWithInput.input.map("userDetails").get("name"), "address",
                                        WorkflowWithInput.input.map("userDetails").get("addressLine"), "orderNo",
                                        WorkflowWithInput.input.map("orderDetail").get("orderNumber")))
                        .defaultCase(new Terminate("unsupported_shipping_type", Workflow.WorkflowStatus.FAILED,
                                "Unsupported Shipping Method")))
                .add(new CustomTask("send_email", "send_email").input("name",
                        WorkflowWithInput.input.map("userDetails").get("name"), "email",
                        WorkflowWithInput.input.map("userDetails").get("email"), "orderNo",
                        WorkflowWithInput.input.map("orderDetail").get("orderNumber")));
        WorkflowWithInput<Order> conductorWorkflow = builder.build();

        var workflowDef = conductorWorkflow.toWorkflowDef();
        manager.registerWorkflow(workflowDef, true);

        return conductorWorkflow;
    }

    public WorkflowWithInput<Shipment> createShipmentWorkflow() {

        WorkflowBuilder<Shipment> builder = new WorkflowBuilder<>();

        CustomTask getOrderDetails = new CustomTask("get_order_details", "get_order_details").input("orderNo",
                WorkflowWithInput.input.get("orderNo"));

        CustomTask getUserDetails = new CustomTask("get_user_details", "get_user_details").input("userId",
                WorkflowWithInput.input.get("userId"));

        WorkflowWithInput<Shipment> conductorWorkflow = builder.name("shipment_workflow").version(1)
                .ownerEmail("user@example.com").variables(new ShipmentState())
                .timeoutPolicy(WorkflowDef.TimeoutPolicy.TIME_OUT_WF, 60) // 30 days
                .description("Workflow to track shipment")
                .add(new ForkJoin("get_in_parallel", new Task[] { getOrderDetails }, new Task[] { getUserDetails }))

                // For all the line items in the order, run in parallel:
                // (calculate tax, charge payment, set state, prepare shipment, send
                // shipment, set state)
                .add(new DynamicFork("process_order",
                        new CustomTask("generateDynamicFork", "generateDynamicFork")
                                .input("orderDetails", getOrderDetails.taskOutput.get("result"))
                                .input("userDetails", getUserDetails.taskOutput)))

                // Update the workflow state with shipped = true
                .add(new SetVariable("update_state").input("shipped", true)).build();

        var workflowDef = conductorWorkflow.toWorkflowDef();
        manager.registerWorkflow(workflowDef, true);

        return conductorWorkflow;
    }

    public static void main(String[] args) {

        String conductorServerURL = "http://localhost:8080/api/"; // Change this to your Conductor server
        WorkflowManager manager = new WorkflowManager(conductorServerURL);

        // Create the new shipment workflow
        ShipmentWorkflow shipmentWorkflow = new ShipmentWorkflow(manager);

        // Create two workflows

        // 1. Order flow that ships an individual order
        // 2. Shipment Workflow that tracks multiple orders in a shipment
        shipmentWorkflow.createOrderFlow();
        WorkflowWithInput<Shipment> workflow = shipmentWorkflow.createShipmentWorkflow();

        // Execute the workflow and wait for it to complete
        try {
            Shipment workflowInput = new Shipment("userA", "order123");

            // Execute returns a completable future.
            CompletableFuture<Workflow> executionFuture = manager.startWorkflow(workflow.getName(), workflow.getVersion(), workflowInput);

            // Wait for a maximum of a minute for the workflow to complete.
            Workflow run = executionFuture.get(1, TimeUnit.MINUTES);

            System.out.println("Workflow Id: " + run);
            System.out.println("Workflow Status: " + run.getStatus());
            System.out.println("Workflow Output: " + run.getOutput());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }

        System.out.println("Done");
    }
}
