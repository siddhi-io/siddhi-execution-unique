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
 * class representing unique Time Window TestCase implementation.
 */
public class UniqueTimeWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueTimeWindowTestCase.class);
    private int inEventCount;
    private boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger eventCount = new AtomicInteger(0);

    @BeforeMethod public void init() {
        inEventCount = 0;
        eventArrived = false;
    }

    /**
     * Commenting out intermittent failing test case until fix this properly.
     */
    @Test public void uniqueTimeWindowTest1() throws InterruptedException {
        log.info("UniqueTimeWindow Test1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(symbol, 2 sec) select symbol,price,"
                        + "volume insert all events into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents != null) {
                    eventCount.addAndGet(inEvents.length);
                    inEventCount = inEventCount + inEvents.length;
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "IBM", 60.5f, 2 });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 2);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueTimeWindowTest2() throws InterruptedException {
        log.info("UniqueTimeWindow Test2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(symbol, 1 sec) select symbol,price,"
                        + "volume insert all events into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "ABC", 60.5f, 2 });
        inputHandler.send(new Object[] { "IBM", 60.4f, 4 });
        Thread.sleep(1100);
        inputHandler.send(new Object[] { "RRR", 700f, 3 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 7 });
        Thread.sleep(1100);
        inputHandler.send(new Object[] { "IBM", 700f, 5 });
        inputHandler.send(new Object[] { "AAA", 60.5f, 6 });

        SiddhiTestHelper.waitForEvents(waitTime, 7, eventCount, timeout);
        Assert.assertEquals(inEventCount, 7);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueTimeWindowTest3() throws InterruptedException {
        log.info("UniqueTimeWindow Test3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(symbol, 1 sec) select symbol,price,"
                        + "volume insert current events into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "ABC", 60.5f, 2 });
        inputHandler.send(new Object[] { "IBM", 60.4f, 4 });
        Thread.sleep(1100);
        inputHandler.send(new Object[] { "RRR", 700f, 3 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 7 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        Assert.assertEquals(inEventCount, 5);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueTimeWindowTest4() throws InterruptedException {
        log.info("UniqueTimeWindow Test4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(volume, 1 sec) select symbol,price,"
                        + "volume insert expired events into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "ABC", 60.5f, 2 });
        inputHandler.send(new Object[] { "IBM", 60.4f, 1 });
        Thread.sleep(1100);
        inputHandler.send(new Object[] { "RRR", 700f, 3 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 7 });
        Thread.sleep(1100);
        inputHandler.send(new Object[] { "IBM", 700f, 5 });
        inputHandler.send(new Object[] { "AAA", 60.5f, 6 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertEquals(inEventCount, 0);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueTimeWindowTest5() throws InterruptedException {
        log.info("UniqueTimeWindow Test5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(symbol, 1 sec) select symbol,price,"
                        + "volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "IBM", 700f, 1 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertEquals(inEventCount, 3);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeWindowTest6() {
        log.info("Test for UniqueTime window's parameter time invalid type");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(symbol, '2 sec') select symbol,price,"
                        + "volume insert all events into outputStream ;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeWindowTest7() {
        log.info("Test for UniqueTime window's parameter should constant case");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(symbol,volume) select symbol,price,"
                        + "volume insert all events into outputStream ;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeWindowTest8()  {
        log.info("Test for UniqueTime window invalid number of parameter");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(symbol) select symbol,price,"
                        + "volume insert all events into outputStream ;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test public void uniqueTimeWindowTest9() throws InterruptedException {
        log.info("UniqueTimeWindow Test for current & restore state");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "@info(name = 'query1') from cseEventStream#window.unique:time(symbol, 2 sec) select symbol,price,"
                        + "volume insert all events into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        inEventCount++;
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(1, event.getData(2));
                        } else if (inEventCount == 2) {
                            AssertJUnit.assertEquals(2, event.getData(2));
                        }
                    }
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        //persisting
        siddhiAppRuntime.persist();
        //restarting execution plan
        siddhiAppRuntime.shutdown();
        inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Error in restoring last revision");
        }
        inputHandler.send(new Object[] { "IBM", 60.5f, 2 });
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        AssertJUnit.assertEquals(inEventCount, 2);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
