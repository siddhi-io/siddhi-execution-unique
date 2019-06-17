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
package io.siddhi.extension.execution.unique;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * class representing a unique first length batch window test case implementation.
 */

public class UniqueFirstLengthBatchWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueFirstLengthBatchWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private int count;
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger eventCount;

    @BeforeMethod public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        count = 0;
        eventCount = new AtomicInteger(0);
    }

    @Test public void uniqueFirstLengthBatchWindowTest2() throws InterruptedException {
        log.info("UniqueFirstLengthBatchWindow test1");
        final int length = 4;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:firstLengthBatch(symbol," + length
                        + ") " + "select symbol, price, volume " + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "WSO2", 61.5f, 2 });
        inputHandler.send(new Object[] { "IBM1", 700f, 3 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 4 });
        inputHandler.send(new Object[] { "IBM3", 700f, 5 });
        inputHandler.send(new Object[] { "WSO22", 60.5f, 6 });
        inputHandler.send(new Object[] { "aa", 60.5f, 7 });
        inputHandler.send(new Object[] { "uu", 60.5f, 8 });
        inputHandler.send(new Object[] { "tt", 60.5f, 9 });
        inputHandler.send(new Object[] { "IBM", 700f, 10 });
        inputHandler.send(new Object[] { "WSO2", 61.5f, 11 });
        inputHandler.send(new Object[] { "IBM1", 700f, 12 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 13 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertEquals(count, 0, "Total event count");
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
