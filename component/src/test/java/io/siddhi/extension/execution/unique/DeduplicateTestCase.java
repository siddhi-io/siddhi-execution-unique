/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * class representing unique first window test case.
 */

public class DeduplicateTestCase {
    private static final Logger log = Logger.getLogger(DeduplicateTestCase.class);
    private int count;
    private boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 30000;
    private int lastValueRemoved;
    private AtomicInteger eventCount;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);
        lastValueRemoved = 0;
    }

    @Test
    public void deduplicateTest1() throws InterruptedException {
        log.info("Deduplicate test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "define stream LoginEvents (timeStamp long, ip string);" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEvents#unique:deduplicate(ip, 1 sec) " +
                "select ip " +
                "insert into UniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
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

        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        Thread.sleep(1001);
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.4"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.4"});
        Thread.sleep(1001);
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.5"});

        SiddhiTestHelper.waitForEvents(0, 4, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(count, 4, "Number of output event value");
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void deduplicateTest2() throws InterruptedException {
        log.info("Deduplicate test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "define stream LoginEvents (timeStamp long, ip string);" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEvents#unique:deduplicate(ip, 1 sec) " +
                "select ip " +
                "insert into UniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
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

        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.12"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});

        SiddhiTestHelper.waitForEvents(0, 2, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(count, 2, "Number of output event value");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void deduplicateTest3() throws InterruptedException {
        log.info("Deduplicate test3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "define stream LoginEvents (timeStamp long, ip string);" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEvents#unique:deduplicate(ip, 1 sec) " +
                "select ip " +
                "insert into UniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
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
        inputHandler.send(new Event[]{
                new Event(System.currentTimeMillis(), new Object[]{System.currentTimeMillis(), "192.10.1.3"}),
                new Event(System.currentTimeMillis(), new Object[]{System.currentTimeMillis(), "192.10.1.12"}),
                new Event(System.currentTimeMillis(), new Object[]{System.currentTimeMillis(), "192.10.1.3"}),
                new Event(System.currentTimeMillis(), new Object[]{System.currentTimeMillis(), "192.10.1.3"}),
                new Event(System.currentTimeMillis(), new Object[]{System.currentTimeMillis(), "192.10.1.3"}),
                new Event(System.currentTimeMillis(), new Object[]{System.currentTimeMillis(), "192.10.1.3"})
        });
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.12"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});

        SiddhiTestHelper.waitForEvents(0, 2, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(count, 2, "Number of output event value");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void deduplicateTest4() throws InterruptedException {
        log.info("deduplicateTest4 - first unique window query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@app:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#unique:deduplicate(symbol, 3 sec)" +
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

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void deduplicateTest5() throws InterruptedException {
        log.info("Deduplicate test5");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "define stream LoginEvents (timeStamp long, ip string);" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEvents#unique:deduplicate(ip, timeStamp) " +
                "select ip " +
                "insert into UniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void deduplicateTest6() throws InterruptedException {
        log.info("Deduplicate test6");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "define stream LoginEvents (timeStamp long, ip string);" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEvents#unique:deduplicate(ip) " +
                "select ip " +
                "insert into UniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
    }
}

