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
package org.wso2.extension.siddhi.window.uniquelengthbatch;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class UniqueLengthBatchWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueLengthBatchWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private int count;
    private boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void LengthBatchWindowTest1() throws InterruptedException {
        log.info("Testing length batch window with no of events smaller than window size");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol,4) " +
                "select symbol,price,volume " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Assert.fail("No events should arrive");
                inEventCount = inEventCount + inEvents.length;
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"ORACLE", 700f, 2});
        Thread.sleep(500);
        Assert.assertEquals(0, inEventCount);
        Assert.assertFalse(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void LengthBatchWindowTest2() throws InterruptedException {
        log.info("Testing length batch window with no of events greater than window size");

        final int length = 4;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol," + length + ") " +
                "select symbol, price, volume " +
                "insert expired events into outputStream ;";

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
        Thread.sleep(500);
        Assert.assertEquals("Total event count", 0, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void lengthWindowBatchTestFirstUnique() throws InterruptedException {

        final int length = 4;
        final boolean isFirstUniqueEnabled = true;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol," + length + "," + isFirstUniqueEnabled + ") " +
                "select symbol,price, volume " +
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
//        for(int i=0;i<=5000000;i++){
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"IBM1", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 61.5f, 6});
        inputHandler.send(new Object[]{"IBM2", 700f, 7});
        inputHandler.send(new Object[]{"WSO23", 60.5f, 8});
        inputHandler.send(new Object[]{"IBM", 700f, 9});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 10});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 11});
        inputHandler.send(new Object[]{"IBM3", 700f, 12});
        inputHandler.send(new Object[]{"WSO23", 60.5f, 13});
        inputHandler.send(new Object[]{"IBM", 700f, 14});
        inputHandler.send(new Object[]{"WSO22", 60.5f, 15});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 16});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 17});
        inputHandler.send(new Object[]{"IBM3", 700f, 18});
        inputHandler.send(new Object[]{"IBM1", 700f, 19});
        inputHandler.send(new Object[]{"WSO23", 61.5f, 20});
//        }
        Assert.assertEquals(16, inEventCount);
        Assert.assertEquals(12, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void LengthBatchWindowTest3() throws InterruptedException {
        log.info("Testing length batch window with no of events greater than window size");

        final int length = 2;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol," + length + ") " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    if ((count / length) % 2 == 1) {
                        removeEventCount++;
                        Assert.assertEquals("Remove event order", removeEventCount, event.getData(2));
                        if (removeEventCount == 1) {
                            Assert.assertEquals("Expired event triggering position", length, inEventCount);
                        }
                    } else {
                        inEventCount++;
                        Assert.assertEquals("In event order", inEventCount, event.getData(2));
                    }
                    count++;
                }
                Assert.assertEquals("No of emitted events at window expiration", inEventCount - length,
                        removeEventCount);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        Thread.sleep(500);
        Assert.assertEquals("In event count", 6, inEventCount);
        Assert.assertEquals("Remove event count", 4, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void LengthBatchWindowTest4() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol,4) " +
                "select symbol,sum(price) as sumPrice,volume " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
//                    Assert.assertEquals("Events cannot be expired", false, event.isExpired());
                    inEventCount++;
//                    if (inEventCount == 1) {
//                        Assert.assertEquals(130.0, event.getData(1));
//                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 10f, 0});
        inputHandler.send(new Object[]{"IBM", 20f, 1});
        inputHandler.send(new Object[]{"IBM", 30f, 0});
        inputHandler.send(new Object[]{"IBM", 40f, 1});
        inputHandler.send(new Object[]{"IBM", 50f, 0});
        inputHandler.send(new Object[]{"IBM", 60f, 1});
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
//        Assert.assertEquals(0, inEventCount);
//        Assert.assertTrue(eventArrived);

    }

    @Test
    public void LengthBatchWindowTest5() throws InterruptedException {

        final int length = 2;
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol," + length + ") " +
                "select symbol,price,volume " +
                "insert expired events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    Assert.assertEquals("Remove event order", count, event.getData(2));
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        Thread.sleep(500);
        Assert.assertEquals("Remove event count", 4, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void LengthBatchWindowTest6() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol,4) " +
                "select symbol,sum(price) as sumPrice,volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    Assert.assertEquals("Events cannot be expired", false, event.isExpired());
                    inEventCount++;
                    if (inEventCount == 1) {
                        Assert.assertEquals(130.0, event.getData(1));
                    } else if (inEventCount == 2) {
                        Assert.assertEquals(130.0, event.getData(1));
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 10f, 1});
        inputHandler.send(new Object[]{"WSO2", 20f, 2});
        inputHandler.send(new Object[]{"IBM1", 30f, 3});
        inputHandler.send(new Object[]{"WSO2", 40f, 4});
        inputHandler.send(new Object[]{"IBM2", 50f, 5});
        inputHandler.send(new Object[]{"WSO2", 60f, 6});
        inputHandler.send(new Object[]{"WSO2", 60f, 7});
        inputHandler.send(new Object[]{"IBM3", 70f, 8});
        inputHandler.send(new Object[]{"WSO2", 80f, 9});
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        Assert.assertEquals(1, inEventCount);
        Assert.assertTrue(eventArrived);
    }

    @Test
    public void LengthBatchWindowTest7() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol,4) " +
                "select symbol,sum(price) as sumPrice,volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Assert.assertEquals("Events cannot be expired", false, removeEvents != null);
                for (Event event : inEvents) {
                    inEventCount++;
                    if (inEventCount == 1) {
                        Assert.assertEquals(130.0, event.getData(1));
                    } else if (inEventCount == 2) {
                        Assert.assertEquals(130.0, event.getData(1));
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 10f, 1});
        inputHandler.send(new Object[]{"WSO2", 20f, 2});
        inputHandler.send(new Object[]{"IBM1", 30f, 3});
        inputHandler.send(new Object[]{"WSO2", 40f, 4});
        inputHandler.send(new Object[]{"IBM2", 50f, 5});
        inputHandler.send(new Object[]{"WSO2", 60f, 6});
        inputHandler.send(new Object[]{"WSO2", 60f, 7});
        inputHandler.send(new Object[]{"IBM3", 70f, 8});
        inputHandler.send(new Object[]{"WSO2", 80f, 9});
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        Assert.assertEquals(1, inEventCount);
        Assert.assertTrue(eventArrived);
    }

    @Test
    public void LengthBatchWindowTest8() throws InterruptedException {
        log.info("LengthBatchWindow Test8");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol,2) join twitterStream#window.unique:lengthBatch(company,2) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            cseEventStreamHandler.send(new Object[]{"IBM", 59.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            twitterStreamHandler.send(new Object[]{"User2", "Hello World2", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            Thread.sleep(500);
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
            Thread.sleep(1000);
            Assert.assertEquals(1, inEventCount);
            Assert.assertEquals(1, removeEventCount);
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void LengthBatchWindowTest9() throws InterruptedException {
        log.info("LengthBatchWindow Test9");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.unique:lengthBatch(symbol,2) join twitterStream#window.unique:lengthBatch(company,2) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            cseEventStreamHandler.send(new Object[]{"IBM", 59.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            twitterStreamHandler.send(new Object[]{"User2", "Hello World2", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            Thread.sleep(500);
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
            Thread.sleep(1000);
            Assert.assertEquals(1, inEventCount);
            Assert.assertEquals(0, removeEventCount);
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }
}