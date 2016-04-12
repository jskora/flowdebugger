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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestFlowDebugger {

    final FlowDebuggerInspectable flowDebugger = new FlowDebuggerInspectable();
    final TestRunner runner = TestRunners.newTestRunner(flowDebugger);
    final ProcessContext context = runner.getProcessContext();

    @Before
    public void setup() throws IOException {
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
        runner.setProperty(FlowDebugger.SUCCESS_PERCENT, "100");
        runner.assertNotValid();
    }

    @Test
    public void testOnEnabled() throws Exception {
        assertEquals(0L, flowDebugger.getSuccessCutoff());
        runner.assertValid();
//        runner.setIncomingConnection(true);
//        runner.addConnection(FlowDebugger.REL_SUCCESS);
//        runner.addConnection(FlowDebugger.REL_FAILURE);
        runner.run();
        assertEquals(context.getProperty(FlowDebugger.SUCCESS_PERCENT).asInteger().intValue(),
                flowDebugger.getSuccessCutoff());
    }

    @Test
    public void testFlowfileRollback() throws IOException {
        final byte[] content = "Hello, World!".getBytes();
        final File sourceFile = new File("testFile1.txt");

        runner.setProperty(CoreAttributes.FILENAME.key(), "testFile1.txt");
        runner.setProperty(CoreAttributes.UUID.key(), "TESTING-1234-TESTING");

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FlowDebugger.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FlowDebugger.REL_SUCCESS).get(0).assertContentEquals(content);

        assertTrue(sourceFile.exists());
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
