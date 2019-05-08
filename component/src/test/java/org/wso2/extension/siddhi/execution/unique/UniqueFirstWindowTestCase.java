/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.execution.unique;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
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
 * class representing unique first window test case.
 */

public class UniqueFirstWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueFirstWindowTestCase.class);
    private int count;
    private boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 30000;
    private int lastValueRemoved;
    private AtomicInteger eventCount;

    @BeforeMethod public void init() {
        count = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);
        lastValueRemoved = 0;
    }

    @Test public void uniqueFirstWindowTest1() throws InterruptedException {
        log.info("UniqueFirstWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:first(ip) " + "select ip "
                + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    count = count + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.fail("Remove events emitted");
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.4" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.5" });

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(count, 3, "Number of output event value");
        siddhiAppRuntime.shutdown();

    }

    @Test public void firstUniqueWindowTest2() throws InterruptedException {
        log.info("UniqueFirstWindow test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:first(ip) " + "select ip "
                + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    count = count + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.fail("Remove events emitted");
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.12" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(count, 2, "Number of output event value");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void firstUniqueWindowTest3() throws InterruptedException {
        log.info("firstUniqueWindowTest3 - first unique window query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@app:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.unique:first(symbol) " +
                "select * " +
                "insert all events into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                eventCount.incrementAndGet();
                for (Event inEvent : inEvents) {
                    lastValueRemoved = (Integer) inEvent.getData(2);
                }
            }
        };

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});
        AssertJUnit.assertTrue(eventArrived);
        Thread.sleep(500);
        AssertJUnit.assertEquals(100, lastValueRemoved);

        //persisting
        executionPlanRuntime.persist();
        Thread.sleep(500);

        inputHandler.send(new Object[]{"MIT", 75.6f, 110});

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        try {
            executionPlanRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Error in restoring last revision");
        }

        inputHandler.send(new Object[]{"MIT", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 110});

        SiddhiTestHelper.waitForEvents(100, 4, eventCount, 10000);
        AssertJUnit.assertEquals(true, eventArrived);
        AssertJUnit.assertEquals(100, lastValueRemoved);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void firstUniqueWindowTest4() throws InterruptedException {
        log.info("firstUniqueWindowTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:first(symbol) join twitterStream#window.unique:ever(user) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                    if (inEvents != null) {
                        for (Event inEvent : inEvents) {
                            eventCount.incrementAndGet();
                        }
                    }
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            cseEventStreamHandler.send(new Object[]{"IBM", 59.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            twitterStreamHandler.send(new Object[]{"User2", "Hello World2", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"WSO2", 75.6f, 100});
            SiddhiTestHelper.waitForEvents(100, 2, eventCount, 10000);
            Assert.assertEquals(2, eventCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

}
