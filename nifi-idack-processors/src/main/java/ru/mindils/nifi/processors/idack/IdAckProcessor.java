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


import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.time.Instant;
import java.util.*;

@Tags({"id", "acknowledgment", "state", "timestamp", "example"})
@CapabilityDescription("A processor to track IDs and timestamps of sent and acknowledged FlowFiles, maintaining state.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@SupportsBatching
@TriggerSerially
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
@Stateful(scopes = {Scope.CLUSTER}, description = "Tracks the last sent and acknowledged IDs along with timestamps.")
public class IdAckProcessor extends AbstractProcessor {

    // Keys for storing state
    private static final String STATE_LAST_SENT_ID = "lastSentId";
    private static final String STATE_LAST_SENT_TIME = "lastSentTime";
    private static final String STATE_LAST_ACK_ID = "lastAcknowledgedId";
    private static final String STATE_LAST_ACK_TIME = "lastAcknowledgedTime";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles with a new ID added.")
            .build();

    public static final Relationship REL_ACK = new Relationship.Builder()
            .name("ack")
            .description("FlowFiles with acknowledged IDs.")
            .build();

    public static final Relationship REL_OTHER = new Relationship.Builder()
            .name("other")
            .description("All other FlowFiles.")
            .build();

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        // Initialize relationships
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_ACK);
        rels.add(REL_OTHER);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        // Return the set of relationships
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Get the FlowFile from the session
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            // Access the state manager and current state map
            StateManager stateManager = context.getStateManager();
            StateMap stateMap = stateManager.getState(Scope.CLUSTER);

            // Retrieve values from the state
            String lastSentId = stateMap.get(STATE_LAST_SENT_ID);
            String lastAckId = stateMap.get(STATE_LAST_ACK_ID);

            // Get the ID attribute from the FlowFile
            String flowFileId = flowFile.getAttribute("idack");

            if (flowFileId == null && (lastSentId == null || lastSentId.equals(lastAckId))) {
                // Case 1: Add a new ID and timestamp if the state is empty or IDs match
                String newId = UUID.randomUUID().toString();
                String currentTime = Instant.now().toString();
                flowFile = session.putAttribute(flowFile, "idack", newId);

                // Update the state with the new ID and timestamp
                Map<String, String> newState = new HashMap<>(stateMap.toMap());
                newState.put(STATE_LAST_SENT_ID, newId);
                newState.put(STATE_LAST_SENT_TIME, currentTime);
                stateManager.setState(newState, Scope.CLUSTER);

                session.transfer(flowFile, REL_SUCCESS);

            } else if (flowFileId != null && flowFileId.equals(lastSentId)) {
                // Case 3: Update the acknowledgment ID and timestamp
                String currentTime = Instant.now().toString();

                Map<String, String> newState = new HashMap<>(stateMap.toMap());
                newState.put(STATE_LAST_ACK_ID, flowFileId);
                newState.put(STATE_LAST_ACK_TIME, currentTime);
                stateManager.setState(newState, Scope.CLUSTER);

                session.transfer(flowFile, REL_ACK);

            } else {
                // Case 2: Forward the FlowFile to REL_OTHER
                session.transfer(flowFile, REL_OTHER);
            }

        } catch (Exception e) {
            getLogger().error("Error processing FlowFile", e);
            session.rollback();
        }
    }
}