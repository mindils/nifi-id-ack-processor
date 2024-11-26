/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mindils.nifi.processors.idack;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class IdAckProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(IdAckProcessor.class);
    }

    /**
     * Test Case 1:
     * When there is no state and a FlowFile without "id" attribute is processed,
     * the processor should add a new "id" attribute, update the state,
     * and route the FlowFile to REL_SUCCESS.
     */
    @Test
    public void testInitialStateAddsIdAndRoutesToSuccess() throws Exception {
        // Enqueue a FlowFile without attributes
        testRunner.enqueue("");

        // Run the processor
        testRunner.run();

        // Verify the FlowFile was routed to REL_SUCCESS
        testRunner.assertAllFlowFilesTransferred(IdAckProcessor.REL_SUCCESS, 1);

        // Get the processed FlowFile
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(IdAckProcessor.REL_SUCCESS).get(0);

        // Verify that an "id" attribute was added
        String idAttribute = flowFile.getAttribute("idack");
        assertNotNull(idAttribute, "The 'id' attribute should be added.");

        // Verify that the state was updated with lastSentId and lastSentTime
        StateMap stateMap = testRunner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(idAttribute, stateMap.get("lastSentId"), "State 'lastSentId' should match the 'id' attribute.");
        assertNotNull(stateMap.get("lastSentTime"), "State 'lastSentTime' should be set.");
    }

    /**
     * Test Case 2:
     * When a FlowFile with "id" attribute equal to lastSentId is processed,
     * the processor should update lastAcknowledgedId and lastAcknowledgedTime,
     * and route the FlowFile to REL_ACK.
     */
    @Test
    public void testAcknowledgmentUpdatesStateAndRoutesToAck() throws Exception {
        // Set initial state with lastSentId
        String lastSentId = "test-id";
        testRunner.getStateManager().setState(Map.of("lastSentId", lastSentId), Scope.CLUSTER);

        // Enqueue a FlowFile with "id" attribute equal to lastSentId
        testRunner.enqueue("", Map.of("idack", lastSentId));

        // Run the processor
        testRunner.run();

        // Verify the FlowFile was routed to REL_ACK
        testRunner.assertAllFlowFilesTransferred(IdAckProcessor.REL_ACK, 1);

        // Verify that the state was updated with lastAcknowledgedId and lastAcknowledgedTime
        StateMap stateMap = testRunner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSentId, stateMap.get("lastAcknowledgedId"), "State 'lastAcknowledgedId' should be updated.");
        assertNotNull(stateMap.get("lastAcknowledgedTime"), "State 'lastAcknowledgedTime' should be set.");
    }

    /**
     * Test Case 3:
     * When a FlowFile with "id" attribute not equal to lastSentId is processed,
     * the processor should route the FlowFile to REL_OTHER.
     */
    @Test
    public void testFlowFileWithDifferentIdRoutesToOther() throws Exception {
        // Set initial state with lastSentId
        String lastSentId = "test-id";
        testRunner.getStateManager().setState(Map.of("lastSentId", lastSentId), Scope.CLUSTER);

        // Enqueue a FlowFile with a different "id" attribute
        testRunner.enqueue("", Map.of("idack", "different-id"));

        // Run the processor
        testRunner.run();

        // Verify the FlowFile was routed to REL_OTHER
        testRunner.assertAllFlowFilesTransferred(IdAckProcessor.REL_OTHER, 1);

        // State should remain unchanged
        StateMap stateMap = testRunner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSentId, stateMap.get("lastSentId"), "State 'lastSentId' should remain unchanged.");
        assertNull(stateMap.get("lastAcknowledgedId"), "State 'lastAcknowledgedId' should remain null.");
    }

    /**
     * Test Case 4:
     * After acknowledgment, processing a new FlowFile without "id" attribute
     * should generate a new "id", update the state, and route to REL_SUCCESS.
     */
    @Test
    public void testNewFlowFileAfterAckAddsNewIdAndRoutesToSuccess() throws Exception {
        // Set initial state with lastSentId and lastAcknowledgedId equal
        String lastSentId = "test-id";
        testRunner.getStateManager().setState(Map.of(
                "lastSentId", lastSentId,
                "lastAcknowledgedId", lastSentId
        ), Scope.CLUSTER);

        // Enqueue a new FlowFile without "id" attribute
        testRunner.enqueue("");

        // Run the processor
        testRunner.run();

        // Verify the FlowFile was routed to REL_SUCCESS
        testRunner.assertAllFlowFilesTransferred(IdAckProcessor.REL_SUCCESS, 1);

        // Get the processed FlowFile
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(IdAckProcessor.REL_SUCCESS).get(0);

        // Verify that a new "id" attribute was added
        String newId = flowFile.getAttribute("idack");
        assertNotNull(newId, "A new 'id' attribute should be added.");

        // Verify that the state was updated with the new lastSentId
        StateMap stateMap = testRunner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(newId, stateMap.get("lastSentId"), "State 'lastSentId' should be updated with the new 'id'.");
    }

    /**
     * Test Case 5:
     * Ensure that both lastSentId and lastAcknowledgedId are maintained in the state.
     */
    @Test
    public void testStateMaintainsBothIds() throws Exception {
        // Initial processing to add new ID
        testRunner.enqueue("");
        testRunner.run();

        // Get the new lastSentId
        MockFlowFile sentFlowFile = testRunner.getFlowFilesForRelationship(IdAckProcessor.REL_SUCCESS).get(0);
        String lastSentId = sentFlowFile.getAttribute("idack");

        // Acknowledge the ID
        testRunner.enqueue("", Map.of("idack", lastSentId));
        testRunner.run();

        // Verify the FlowFile was routed to REL_ACK
        testRunner.assertTransferCount(IdAckProcessor.REL_ACK, 1);

        // Verify that both IDs are present in the state
        StateMap stateMap = testRunner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSentId, stateMap.get("lastSentId"), "State 'lastSentId' should be present.");
        assertEquals(lastSentId, stateMap.get("lastAcknowledgedId"), "State 'lastAcknowledgedId' should be present.");
    }

    /**
     * Test Case 6:
     * When lastSentId is not equal to lastAcknowledgedId, and a FlowFile without "id" is processed,
     * it should not generate a new ID and should route the FlowFile to REL_OTHER.
     */
    @Test
    public void testUnacknowledgedIdDoesNotGenerateNewId() throws Exception {
        // Set initial state with different lastSentId and lastAcknowledgedId
        testRunner.getStateManager().setState(Map.of(
                "lastSentId", "unacknowledged-id",
                "lastAcknowledgedId", "some-other-id"
        ), Scope.CLUSTER);

        // Enqueue a FlowFile without "id" attribute
        testRunner.enqueue("");

        // Run the processor
        testRunner.run();

        // Verify the FlowFile was routed to REL_OTHER
        testRunner.assertAllFlowFilesTransferred(IdAckProcessor.REL_OTHER, 1);

        // Get the processed FlowFile
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(IdAckProcessor.REL_OTHER).get(0);

        // Verify that no "id" attribute was added
        assertNull(flowFile.getAttribute("idack"), "No 'idack' attribute should be added.");

        // State should remain unchanged
        StateMap stateMap = testRunner.getStateManager().getState(Scope.CLUSTER);
        assertEquals("unacknowledged-id", stateMap.get("lastSentId"), "State 'lastSentId' should remain unchanged.");
        assertEquals("some-other-id", stateMap.get("lastAcknowledgedId"), "State 'lastAcknowledgedId' should remain unchanged.");
    }

    /**
     * Test Case 7:
     * Ensure that timestamps are correctly updated in the state when IDs are sent and acknowledged.
     */
    @Test
    public void testTimestampsAreUpdated() throws Exception {
        // Enqueue a FlowFile without "id" attribute
        testRunner.enqueue("");

        // Run the processor to send a new ID
        testRunner.run();

        // Get the state after sending
        StateMap stateMapAfterSend = testRunner.getStateManager().getState(Scope.CLUSTER);

        String lastSentId = stateMapAfterSend.get("lastSentId");
        String lastSentTime = stateMapAfterSend.get("lastSentTime");
        assertNotNull(lastSentTime, "State 'lastSentTime' should be set after sending.");

        // Enqueue a FlowFile to acknowledge the ID
        testRunner.enqueue("", Map.of("idack", lastSentId));

        // Run the processor to acknowledge the ID
        testRunner.run();

        // Get the state after acknowledgment
        StateMap stateMapAfterAck = testRunner.getStateManager().getState(Scope.CLUSTER);

        String lastAckId = stateMapAfterAck.get("lastAcknowledgedId");
        String lastAckTime = stateMapAfterAck.get("lastAcknowledgedTime");
        assertNotNull(lastAckTime, "State 'lastAcknowledgedTime' should be set after acknowledgment.");

        // Verify that the timestamps are not null and in correct order
        assertEquals(lastSentId, lastAckId, "Acknowledged ID should match the last sent ID.");
        assertTrue(lastAckTime.compareTo(lastSentTime) >= 0, "Acknowledged time should be equal or after the sent time.");
    }
}
