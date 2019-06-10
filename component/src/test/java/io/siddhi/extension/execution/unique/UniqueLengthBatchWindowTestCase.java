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
import io.siddhi.core.stream.output.StreamCallback;
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
 * class representing unique length batch window test case implementation.
 */

public class UniqueLengthBatchWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueLengthBatchWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private int count;
    private boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger eventCount;

    @BeforeMethod public void init() {
        count = 0;
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);
    }

    @Test public void uniqueLengthBatchWindowTest1() throws InterruptedException {
        log.info("Testing length batch window with no of events greater than window size");

        final int length = 4;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol," + length + ") "
                        + "select symbol, price, volume " + "insert expired events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                eventCount.incrementAndGet();
                if (inEvents != null) {
                    eventCount.incrementAndGet();
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
        Thread.sleep(500);
        Assert.assertEquals(count, 0, "Total event count");
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueLengthBatchWindowTest2() throws InterruptedException {
        log.info("Testing length batch window with no of events greater than window size");

        final int length = 2;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol," + length + ") "
                        + "select symbol,price,volume " + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    if ((count / length) % 2 != 0) {
                        removeEventCount++;
                        Assert.assertEquals(removeEventCount, event.getData(2), "Remove event order");
                        if (removeEventCount == 1) {
                            AssertJUnit.assertEquals("Expired event triggering position", length, inEventCount);
                        }
                    } else {
                        eventCount.incrementAndGet();
                        inEventCount++;
                        AssertJUnit.assertEquals("In event order", inEventCount, event.getData(2));
                    }
                    count++;
                }
                AssertJUnit.assertEquals("No of emitted events at window expiration", inEventCount - length,
                        removeEventCount);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 2 });
        inputHandler.send(new Object[] { "IBM", 700f, 3 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 4 });
        inputHandler.send(new Object[] { "IBM", 700f, 5 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 6 });

        SiddhiTestHelper.waitForEvents(waitTime, 6, eventCount, timeout);
        AssertJUnit.assertEquals("In event count", 6, inEventCount);
        AssertJUnit.assertEquals("Remove event count", 4, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueLengthBatchWindowTest3() throws InterruptedException {
        log.info("UniqueLengthBatchWindow Test3");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol,4) "
                + "select symbol,price,volume " + "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                eventCount.incrementAndGet();
                if (events != null) {
                    inEventCount += events.length;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 10f, 0 });
        inputHandler.send(new Object[] { "IBM", 20f, 1 });
        inputHandler.send(new Object[] { "IBM", 30f, 0 });
        inputHandler.send(new Object[] { "IBM", 40f, 1 });
        inputHandler.send(new Object[] { "IBM", 50f, 0 });
        inputHandler.send(new Object[] { "IBM", 60f, 1 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(0, inEventCount);
        AssertJUnit.assertTrue(!eventArrived);

    }

    @Test public void uniqueLengthBatchWindowTest4() throws InterruptedException {
        log.info("UniqueLengthBatchWindow Test4");

        final int length = 2;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query =
                "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol," + length + ") "
                        + "select symbol,price,volume " + "insert expired events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override public void receive(Event[] events) {
                EventPrinter.print(events);

                for (Event event : events) {
                    eventCount.incrementAndGet();
                    count++;
                    AssertJUnit.assertEquals("Remove event order", count, event.getData(2));
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 1 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 2 });
        inputHandler.send(new Object[] { "IBM", 700f, 3 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 4 });
        inputHandler.send(new Object[] { "IBM", 700f, 5 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 6 });

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        Assert.assertEquals(count, 4, "Remove event count");
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueLengthBatchWindowTest5() throws InterruptedException {
        log.info("UniqueLengthBatchWindow Test5");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol,4) "
                + "select symbol,sum(price) as sumPrice,volume " + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    AssertJUnit.assertEquals("Events cannot be expired", false, event.isExpired());
                    inEventCount++;
                    eventCount.incrementAndGet();
                    if (inEventCount == 1) {
                        AssertJUnit.assertEquals(130.0, event.getData(1));
                    } else if (inEventCount == 2) {
                        AssertJUnit.assertEquals(130.0, event.getData(1));
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 10f, 1 });
        inputHandler.send(new Object[] { "WSO2", 20f, 2 });
        inputHandler.send(new Object[] { "IBM1", 30f, 3 });
        inputHandler.send(new Object[] { "WSO2", 40f, 4 });
        inputHandler.send(new Object[] { "IBM2", 50f, 5 });
        inputHandler.send(new Object[] { "WSO2", 60f, 6 });
        inputHandler.send(new Object[] { "WSO2", 60f, 7 });
        inputHandler.send(new Object[] { "IBM3", 70f, 8 });
        inputHandler.send(new Object[] { "WSO2", 80f, 9 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(inEventCount, 1);
        Assert.assertTrue(eventArrived);
    }

    @Test public void uniqueLengthBatchWindowTest6() throws InterruptedException {
        log.info("UniqueLengthBatchWindow Test6");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol,4) "
                + "select symbol,sum(price) as sumPrice,volume " + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertEquals("Events cannot be expired", false, removeEvents != null);
                for (Event event : inEvents) {
                    inEventCount++;
                    eventCount.incrementAndGet();
                    if (inEventCount == 1) {
                        AssertJUnit.assertEquals(130.0, event.getData(1));
                    } else if (inEventCount == 2) {
                        AssertJUnit.assertEquals(130.0, event.getData(1));
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 10f, 1 });
        inputHandler.send(new Object[] { "WSO2", 20f, 2 });
        inputHandler.send(new Object[] { "IBM1", 30f, 3 });
        inputHandler.send(new Object[] { "WSO2", 40f, 4 });
        inputHandler.send(new Object[] { "IBM2", 50f, 5 });
        inputHandler.send(new Object[] { "WSO2", 60f, 6 });
        inputHandler.send(new Object[] { "WSO2", 60f, 7 });
        inputHandler.send(new Object[] { "IBM3", 70f, 8 });
        inputHandler.send(new Object[] { "WSO2", 80f, 9 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(inEventCount, 1);
        Assert.assertTrue(eventArrived);
    }

    @Test public void uniqueLengthBatchWindowTest7() throws InterruptedException {
        log.info("UniqueLengthBatchWindow Test7");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" + "define stream cseEventStream (symbol string, price float, volume int); "
                + "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol,2) "
                + "join twitterStream#window.unique:lengthBatch(company,2) "
                + "on cseEventStream.symbol== twitterStream.company "
                + "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
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
            cseEventStreamHandler.send(new Object[] { "WSO2", 55.6f, 100 });
            cseEventStreamHandler.send(new Object[] { "IBM", 59.6f, 100 });
            twitterStreamHandler.send(new Object[] { "User1", "Hello World", "WSO2" });
            twitterStreamHandler.send(new Object[] { "User2", "Hello World2", "WSO2" });
            cseEventStreamHandler.send(new Object[] { "IBM", 75.6f, 100 });
            Thread.sleep(500);
            cseEventStreamHandler.send(new Object[] { "WSO2", 57.6f, 100 });

            SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
            Assert.assertEquals(inEventCount, 1);
            Assert.assertEquals(removeEventCount, 1);
            Assert.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test public void uniqueLengthBatchWindowTest8() throws InterruptedException {
        log.info("UniqueLengthBatchWindow Test8");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" + "define stream cseEventStream (symbol string, price float, volume int); "
                + "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol,2) "
                + "join twitterStream#window.unique:lengthBatch(company,2) "
                + "on cseEventStream.symbol== twitterStream.company "
                + "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price "
                + "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);

                    if (inEvents != null) {
                        eventCount.addAndGet(inEvents.length);
                        inEventCount += (inEvents.length);
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
            cseEventStreamHandler.send(new Object[] { "WSO2", 55.6f, 100 });
            cseEventStreamHandler.send(new Object[] { "IBM", 59.6f, 100 });
            twitterStreamHandler.send(new Object[] { "User1", "Hello World", "WSO2" });
            twitterStreamHandler.send(new Object[] { "User2", "Hello World2", "WSO2" });
            cseEventStreamHandler.send(new Object[] { "IBM", 75.6f, 100 });
            Thread.sleep(500);
            cseEventStreamHandler.send(new Object[] { "WSO2", 57.6f, 100 });

            SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
            Assert.assertEquals(inEventCount, 1);
            Assert.assertEquals(removeEventCount, 0);
            Assert.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueLengthBatchWindowTest10() {
        log.info("Test for Unique Length Batch window's Length invalid type case");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol,'4') "
                + "select symbol, price, volume " + "insert expired events into outputStream ;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueLengthBatchWindowTest11() {
        log.info("Test for Unique Length Batch window should constant case");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol,volume) "
                + "select symbol, price, volume " + "insert expired events into outputStream ;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueLengthBatchWindowTest12() {
        log.info("Test for invalid number of parameter in Unique Length Batch window case");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol) "
                + "select symbol, price, volume " + "insert expired events into outputStream ;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void uniqueLengthBatchWindowTest13() throws InterruptedException {
        log.info("Testing length batch window for restore & current state");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cseEventStream = "" + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " + "from cseEventStream#window.unique:lengthBatch(symbol,4) "
                + "select symbol,sum(price) as sumPrice,volume " + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    inEventCount++;
                    eventCount.incrementAndGet();
                    if (inEventCount == 1) {
                        AssertJUnit.assertEquals(130.0, event.getData(1));
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "IBM", 10f, 1 });
        inputHandler.send(new Object[] { "WSO2", 20f, 2 });
        inputHandler.send(new Object[] { "IBM1", 30f, 3 });
        inputHandler.send(new Object[] { "WSO2", 40f, 4 });
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
        inputHandler.send(new Object[] { "IBM2", 50f, 5 });
        inputHandler.send(new Object[] { "WSO2", 60f, 6 });
        inputHandler.send(new Object[] { "WSO2", 60f, 7 });
        inputHandler.send(new Object[] { "IBM3", 70f, 8 });
        inputHandler.send(new Object[] { "WSO2", 80f, 9 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(inEventCount, 1);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
