/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.execution.unique;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class UniqueFirstLengthBatchWindowTestCase {
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private int count;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        count = 0;
    }

    @Test
    public void LengthBatchWindowTest2() throws InterruptedException {
        final int length = 4;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:firstLengthBatch(symbol," + length + ") " +
                "select symbol, price, volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 61.5f, 2});
        inputHandler.send(new Object[]{"IBM1", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        inputHandler.send(new Object[]{"IBM3", 700f, 5});
        inputHandler.send(new Object[]{"WSO22", 60.5f, 6});
        inputHandler.send(new Object[]{"aa", 60.5f, 7});
        inputHandler.send(new Object[]{"uu", 60.5f, 8});
        inputHandler.send(new Object[]{"tt", 60.5f, 9});
        inputHandler.send(new Object[]{"IBM", 700f, 10});
        inputHandler.send(new Object[]{"WSO2", 61.5f, 11});
        inputHandler.send(new Object[]{"IBM1", 700f, 12});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 13});
        Thread.sleep(5000);
        Assert.assertEquals("Total event count", 0, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}
