/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.execution.unique;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * class representing unique length window test case implementation.
 */
public class UniqueLengthWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueLengthWindowTestCase.class);
    private int count;
    private long value;
    private boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger eventCount;

    @BeforeMethod public void init() {
        count = 0;
        value = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);
    }

    @Test public void uniqueLengthWindowTest1() throws InterruptedException {
        log.info("Testing uniqueLength window with no of events smaller than window size");
        final int length = 4;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, a string, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:length(ip," + length + ") "
                + "select a, count(ip) as ipCount, ip " + "insert all events into uniqueIps ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    value = (Long) inEvents[inEvents.length - 1].getData(1);
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A1", "192.10.1.4" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A2", "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A3", "192.10.1.3" });

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(value, 2, "Event max value");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueLengthWindowTest2() throws InterruptedException {
        log.info("Testing uniqueLength window with no of events greater than window size");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, a string, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:length(ip,3) "
                + "select a, count(ip) as ipCount, ip " + "insert all events into uniqueIps ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    value = (Long) inEvents[inEvents.length - 1].getData(1);
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A1", "192.10.1.4" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A2", "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A3", "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A4", "192.10.1.4" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A5", "192.10.1.5" });

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(value, 3, "Event max value");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueLengthWindowTest3() throws InterruptedException {
        log.info("Testing uniqueLength window with no of events greater than window size");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, a string, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:length(ip,3) " + "select a, ip "
                + "insert expired events into uniqueIps ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    value = (Long) inEvents[inEvents.length - 1].getData(1);
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A1", "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A2", "192.10.1.4" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A3", "192.10.1.4" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A4", "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A5", "192.10.1.5" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(value, 0, "Event max value");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueLengthWindowTest4() throws InterruptedException {
        log.info("Testing uniqueLength window with no of events greater than window size");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, a string, ip string, b double);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:length(b,3) "
                + "select a, count(ip) as ipCount, ip, b " + "insert expired events into uniqueIps ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    value = (Long) inEvents[inEvents.length - 1].getData(1);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A1", "192.10.1.4", 2.112 });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A2", "192.10.1.3", 4.33 });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A3", "192.10.1.3", 4.333 });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A4", "192.10.1.4", 5.3 });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A5", "192.10.1.5", 6.1 });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(value, 0, "Event max value");
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueLengthWindowTest5() {
        log.info("Testing uniqueLength window with no of events smaller than window size");
        final int length = 4;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, a string, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:length(ip) "
                + "select a, count(ip) as ipCount, ip " + "insert all events into uniqueIps ;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void uniqueLengthWindowTest6() throws InterruptedException {
        log.info("Testing uniqueLength window for current state restore state.");
        final int length = 4;

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, a string, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:length(ip," + length + ") "
                + "select a, count(ip) as ipCount, ip " + "insert all events into uniqueIps ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        if (eventCount.get() == 1) {
                            AssertJUnit.assertEquals("192.10.1.4", event.getData(2));
                        } else if (eventCount.get() == 2) {
                            AssertJUnit.assertEquals("192.10.1.3", event.getData(2));
                        } else if (eventCount.get() == 3) {
                            AssertJUnit.assertEquals("192.10.1.3", event.getData(2));
                        }
                    }
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A1", "192.10.1.4" });
        siddhiAppRuntime.persist();
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        //restarting execution plan
        siddhiAppRuntime.shutdown();
        inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Error in restoring last revision");
        }
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A2", "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "A3", "192.10.1.3" });

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

}
