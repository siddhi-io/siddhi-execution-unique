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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.execution.unique;

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
 * class representing unique time length batch window test case.
 */
public class UniqueTimeLengthBatchWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueTimeLengthBatchWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private int waitTime = 5000;
    private int timeout = 30000;
    private AtomicInteger eventCount;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);

    }

    @Test
    public void uniqueTimeLengthWindowBatchTest1() throws InterruptedException {
        log.info("timeLengthBatchWindow Test1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol, 1 sec, 5) " +
                "select symbol, price, volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                }
                if (removeEvents != null) {
                    Assert.assertTrue(inEventCount > removeEventCount, "InEvents arrived before RemoveEvents");
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        Thread.sleep(2500);
        inputHandler.send(new Object[]{"WSO2", 61.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        Assert.assertEquals(inEventCount, 5);
        Assert.assertEquals(removeEventCount, 5);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void uniqueTimeLengthWindowBatchTest15() throws InterruptedException {
        log.info("timeLengthBatchWindow Test15");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol, 1 sec, 2) " +
                "select symbol, price, volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                }
                if (removeEvents != null) {
                    Assert.assertTrue(inEventCount > removeEventCount, "InEvents arrived before RemoveEvents");
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        Thread.sleep(2500);
        inputHandler.send(new Object[]{"WSO2", 61.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"GOOG", 60.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"IBM", 705f, 9});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        Assert.assertEquals(inEventCount, 5);
        Assert.assertEquals(removeEventCount, 3);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
        Thread.sleep(3000);
    }

    @Test
    public void uniqueTimeLengthWindowBatchTest2() throws InterruptedException {
        log.info("UniquetimeLengthBatchWindow Test2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol, 1 sec, 3) " +
                "select symbol, sum(price) as price " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                Assert.assertTrue(removeEvents == null, "Remove events should not arrive ");
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        Assert.assertEquals(inEventCount, 3);
        Assert.assertEquals(removeEventCount, 0);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void uniqueTimeLengthWindowBatchTest3() throws InterruptedException {
        log.info("UniquetimeLengthBatchWindow Test3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol,1 sec, 2) " +
                "select symbol, sum(price) as price,volume " +
                "insert expired events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    eventCount.addAndGet(removeEvents.length);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                Assert.assertTrue(inEvents == null, "inEvents should not arrive ");
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 61.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        Assert.assertEquals(inEventCount, 0);
        Assert.assertEquals(removeEventCount, 3);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void uniqueTimeLengthWindowBatchTest4() throws InterruptedException {
        log.info("UniquetimeLengthBatchWindow Test4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" + "define stream cseEventStream (symbol string, price float, volume int); "
                + "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol,1 sec, 3)"
                + " join twitterStream#window.unique:timeLengthBatch(company,1 sec, 3) "
                + "on cseEventStream.symbol== twitterStream.company "
                + "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);

                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                        eventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            Thread.sleep(1100);
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            Assert.assertTrue(inEventCount == 1 || inEventCount == 2, "In Events can be 1 or 2 ");
            Assert.assertTrue(removeEventCount == 1 || removeEventCount == 2, "Removed Events can be 1 or 2 ");
            Assert.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void uniqueTimeLengthWindowBatchTest5() throws InterruptedException {
        log.info("UniquetimeLengthBatchWindow Test5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" + "define stream cseEventStream (symbol string, price float, volume int); "
                + "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol,1 sec, 3) "
                + "join twitterStream#window.unique:timeLengthBatch(company,1 sec, 3) "
                + "on cseEventStream.symbol== twitterStream.company "
                + "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price "
                + "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);

                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                        eventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            Thread.sleep(1500);
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            Assert.assertTrue(inEventCount == 1 || inEventCount == 2, "In Events can be 1 or 2 ");
            Assert.assertEquals(removeEventCount, 0);
            Assert.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void uniqueTimeLengthWindowBatchTest6() throws InterruptedException {
        log.info("UniquetimeLengthBatchWindow Test6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol,2 sec, 2) "
                + "select symbol, sum(price) as sumPrice, volume " + "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEventCount == 0) {
                    Assert.assertTrue(removeEvents == null,
                            "Remove Events will only arrive after the second time period. ");
                }
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                } else if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });
        Thread.sleep(2000);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        // Start sending events in the beginning of a cycle
        while (System.currentTimeMillis() % 2000 != 0) {

        }
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(8100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"II", 60.5f, 1});
        Thread.sleep(13100);
        inputHandler.send(new Object[]{"TT", 60.5f, 1});
        inputHandler.send(new Object[]{"YY", 60.5f, 1});

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        Assert.assertEquals(inEventCount, 3);
        Assert.assertEquals(removeEventCount, 0);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeLengthWindowBatchTest7() {
        log.info("timeLengthBatchWindow Test for Unique Length Batch window should variable case ");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch('symbol',1 sec, 2) "
                + "select symbol, price, volume " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeLengthWindowBatchTest8() {
        log.info("timeLengthBatchWindow Test for Unique Time Batch window invalid type");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol,'1 sec', 2) "
                + "select symbol, price, volume " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeLengthWindowBatchTest9() {
        log.info("timeLengthBatchWindow Test for Unique Time Batch window Constant case ");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol,volume) "
                + "select symbol, price, volume " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeLengthWindowBatchTest10() {
        log.info("timeLengthBatchWindow Test for invalid number of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol) "
                + "select symbol, price, volume " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeLengthWindowBatchTest11() {
        log.info("timeLengthBatchWindow Test for Unique Length Batch window variable case length three");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "" + "@info(name = 'query1') " +
                        "from cseEventStream#window.unique:timeLengthBatch('symbol',1 sec,volume) "
                        + "select symbol, price, volume " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeLengthWindowBatchTest12() {
        log.info("timeLengthBatchWindow Test for UniquetimeLengthBatch window invalid type case length three");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "" + "@info(name = 'query1') " +
                        "from cseEventStream#window.unique:timeLengthBatch(symbol,'1 sec',volume, 5) "
                        + "select symbol, price, volume " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeLengthWindowBatchTest13() {
        log.info("timeLengthBatchWindow Test for Unique Time Batch window should constant case length three");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "" + "@info(name = 'query1') " +
                        "from cseEventStream#window.unique:timeLengthBatch(symbol,price,volume, 6) "
                        + "select symbol, price, volume " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueTimeLengthWindowBatchTest16() {
        log.info("timeLengthBatchWindow Test for Unique Time Batch window should constant case length four");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "" + "@info(name = 'query1') " +
                        "from cseEventStream#window.unique:timeLengthBatch(symbol,1 sec, 0, volume) "
                        + "select symbol, price, volume " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test
    public void uniqueTimeLengthWindowBatchTest14() throws InterruptedException {
        log.info("timeLengthBatchWindow Test1");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#window.unique:timeLengthBatch(symbol,1 sec, 5) "
                + "select symbol, price, volume " + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                }
                if (removeEvents != null) {
                    Assert.assertTrue(inEventCount > removeEventCount, "InEvents arrived before RemoveEvents");
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        SiddhiTestHelper.waitForEvents(2500, 1, eventCount, timeout);
        inputHandler.send(new Object[]{"WSO2", 61.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        SiddhiTestHelper.waitForEvents(1100, 3, eventCount, timeout);
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        SiddhiTestHelper.waitForEvents(waitTime, 6, eventCount, timeout);
        Assert.assertEquals(inEventCount, 5);
        siddhiAppRuntime.persist();
        siddhiAppRuntime.shutdown();
        inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Error in restoring last revision");
        }
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        SiddhiTestHelper.waitForEvents(3500, 6, eventCount, timeout);
        inputHandler.send(new Object[]{"WSO2", 61.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        SiddhiTestHelper.waitForEvents(waitTime, 8, eventCount, timeout);
        Assert.assertEquals(inEventCount, 8);
        Assert.assertEquals(removeEventCount, 7);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
