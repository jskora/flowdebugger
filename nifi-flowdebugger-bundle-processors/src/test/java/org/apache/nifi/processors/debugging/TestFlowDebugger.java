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
package org.apache.nifi.processors.debugging;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class TestFlowDebugger {

    private FlowDebuggerInspectable flowDebugger;
    private TestRunner runner;
    private ProcessContext context;
    private ProcessSession session;

    @Before
    public void setup() throws IOException {
        flowDebugger = new FlowDebuggerInspectable();
        runner = TestRunners.newTestRunner(flowDebugger);
        context = runner.getProcessContext();
        session = runner.getProcessSessionFactory().createSession();
    }

    @Test
    public void testGetSupportedPropertyDescriptors() throws Exception {

    }

    @Test
    public void testGetRelationships() throws Exception {

    }

    @Test
    public void testCustomValidate() throws Exception {
        runner.assertValid();

        // valid combinations

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "100");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "0");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "0");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "0");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "0");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "100");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "0");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "0");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "0");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "0");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "100");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "0");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "0");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "0");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "0");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "100");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "0");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "0");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "0");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "0");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "100");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "60");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "10");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "10");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "10");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "10");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "10");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "60");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "10");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "10");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "10");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "10");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "10");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "60");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "10");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "10");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "10");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "10");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "10");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "60");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "10");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "10");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "10");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "10");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "10");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "60");
        runner.assertValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "80");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "10");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "8");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "2");
        runner.assertValid();

        // invalid combitions

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "0");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "0");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "0");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "0");
        runner.assertNotValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "10");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "10");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "10");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "10");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "10");
        runner.assertNotValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "100");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "100");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "100");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "100");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "100");
        runner.assertNotValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "80");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "10");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "8");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "1");
        runner.assertNotValid();

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "80");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "10");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "8");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "3");
        runner.assertNotValid();
    }

    @Test
    public void testOnScheduled() throws Exception {
        assertEquals(0L, flowDebugger.getSuccessCutoff());
        runner.assertValid();
        runner.run();
        assertEquals(context.getProperty(FlowDebugger.SUCCESS_PERCENT).asInteger().intValue(),
                flowDebugger.getSuccessCutoff());
    }

    @Test
    public void testFlowfileRollback() throws IOException {
        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "0");
        runner.setProperty(FlowDebugger.FAILURE_PERCENT, "0");
        runner.setProperty(FlowDebugger.YIELD_PERCENT, "0");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "100");
        runner.setProperty(FlowDebugger.PENALIZE_PERCENT, "0");
        runner.assertValid();

        Map<String, String> attribs1 = new HashMap<>();
        final byte[] content1 = "Hello, World 1!".getBytes();
        attribs1.put(CoreAttributes.FILENAME.name(), "testFile1.txt");
        attribs1.put(CoreAttributes.UUID.name(), "TESTING-1234-TESTING");
        runner.enqueue(content1, attribs1);

        Map<String, String> attribs2 = new HashMap<>();
        final byte[] content2 = "Hello, World 2!".getBytes();
        attribs2.put(CoreAttributes.FILENAME.name(), "testFile2.txt");
        attribs2.put(CoreAttributes.UUID.name(), "TESTING-2345-TESTING");
        runner.enqueue(content2, attribs2);

        Map<String, String> attribs3 = new HashMap<>();
        final byte[] content3 = "Hello, World 3!".getBytes();
        attribs3.put(CoreAttributes.FILENAME.name(), "testFile3.txt");
        attribs3.put(CoreAttributes.UUID.name(), "TESTING-3456-TESTING");
        runner.enqueue(content3, attribs3);

        runner.run(4);
        runner.assertTransferCount(FlowDebugger.REL_SUCCESS, 0);
        runner.assertTransferCount(FlowDebugger.REL_FAILURE, 0);

        runner.assertQueueNotEmpty();
        assertEquals(3, runner.getQueueSize().getObjectCount());

        MockFlowFile ff1 = (MockFlowFile)session.get();
        assertNotNull(ff1);
        assertEquals(content1, ff1.toByteArray());

        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "100");
        runner.setProperty(FlowDebugger.ROLLBACK_PERCENT, "0");
        runner.run(4);
        runner.assertTransferCount(FlowDebugger.REL_SUCCESS, 3);
        runner.assertTransferCount(FlowDebugger.REL_FAILURE, 0);

        runner.getFlowFilesForRelationship(FlowDebugger.REL_SUCCESS).get(0).assertContentEquals(content1);
        runner.getFlowFilesForRelationship(FlowDebugger.REL_SUCCESS).get(1).assertContentEquals(content2);
        runner.getFlowFilesForRelationship(FlowDebugger.REL_SUCCESS).get(2).assertContentEquals(content3);
    }

    private class FlowDebuggerInspectable extends FlowDebugger {
        public int getSuccessCutoff() {
            return this.SUCCESS_CUTOFF;
        }

        public int getFailureCutoff() {
            return this.FAILURE_CUTOFF;
        }

        public int getYieldCutoff() {
            return this.YIELD_CUTOFF;
        }

        public int getRollbackCutoff() {
            return this.ROLLBACK_CUTOFF;
        }
    }
}
