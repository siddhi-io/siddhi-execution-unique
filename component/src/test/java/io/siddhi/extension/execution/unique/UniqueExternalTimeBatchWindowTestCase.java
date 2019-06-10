/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * class representing UniqueExternalTimeBatchWindowTestCase.
 */
public class UniqueExternalTimeBatchWindowTestCase {

    private static final Logger log = Logger.getLogger(UniqueExternalTimeBatchWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private long sum;
    private long waitTime = 50;
    private long timeout = 30000;
    private AtomicInteger eventCount;

    @BeforeMethod public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        sum = 0;
        eventCount = new AtomicInteger(0);
    }

    @Test public void uniqueExternalTimeBatchWindowTest1() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindowTest test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, 2 sec) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents == null) {
                    eventCount.incrementAndGet();
                }
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        inEventCount++;
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(3L, event.getData(2));
                        } else if (inEventCount == 2) {
                            AssertJUnit.assertEquals(2L, event.getData(2));
                        } else if (inEventCount == 3) {
                            AssertJUnit.assertEquals(3L, event.getData(2));
                        } else if (inEventCount == 4) {
                            AssertJUnit.assertEquals(4L, event.getData(2));
                        } else if (inEventCount == 5) {
                            AssertJUnit.assertEquals(2L, event.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6" });
        Thread.sleep(2100);

        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.8" });
        Thread.sleep(2100);

        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.92" });
        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9" });
        inputHandler.send(new Object[] { 1366335806690L, "192.10.1.10" });

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 5, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueExternalTimeBatchWindowTest2() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip,timestamp, 1 sec, 0, 6 sec) "
                + "select timestamp, ip, count() as total  " + "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804342L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804342L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335814341L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335814345L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335824341L, "192.10.1.7" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 2, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueExternalTimeBatchWindowTest3() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip,timestamp, 1 sec) "
                + "select timestamp, ip, count() as total  " + "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804342L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805340L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335814341L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335814741L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335814641L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335814545L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335824341L, "192.10.1.7" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 2, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();

    }

    @Test public void uniqueExternalTimeBatchWindowTest4() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec) "
                + "select timestamp, ip, count() as total  " + "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }

                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        inEventCount++;
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(2L, inEvent.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804342L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805341L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335814341L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335814345L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335824341L, "192.10.1.7" });

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 3, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();

    }

    @Test public void uniqueExternalTimeBatchWindowTest5() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test5");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip,timestamp, 1 sec, 0, 3 sec) "
                + "select timestamp, ip, count() as total  " + "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }
                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        inEventCount++;

                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(4L, inEvent.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 1, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();

    }

    @Test public void uniqueExternalTimeBatchWindowTest6() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test6");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip,timestamp, 1 sec, 0, 3 sec) "
                + "select timestamp, ip, count() as total  " + "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        eventCount.incrementAndGet();
                        inEventCount++;
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(4L, inEvent.getData(2));
                        } else if (inEventCount == 2) {
                            AssertJUnit.assertEquals(3L, inEvent.getData(2));
                        }
                    }
                }

                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 2, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();

    }

    @Test public void uniqueExternalTimeBatchWindowTest7() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test7");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip,timestamp, 1 sec, 0, 2 sec) "
                + "select timestamp, ip, count() as total  " + "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }

                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        inEventCount++;
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(4L, inEvent.getData(2));
                        } else if (inEventCount == 2) {
                            AssertJUnit.assertEquals(3L, inEvent.getData(2));
                        } else if (inEventCount == 3) {
                            AssertJUnit.assertEquals(5L, inEvent.getData(2));
                        } else if (inEventCount == 4) {
                            AssertJUnit.assertEquals(2L, inEvent.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6" });
        Thread.sleep(3000);
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.8" });
        Thread.sleep(3000);
        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9" });
        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9" });
        inputHandler.send(new Object[] { 1366335806690L, "192.10.1.10" });

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 4, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();

    }

    @Test public void uniqueExternalTimeBatchWindowTest8() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test8");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip,timestamp, 1 sec, 0, 2 sec) "
                + "select timestamp, ip, count() as total  " + "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents == null) {
                    eventCount.incrementAndGet();
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                for (Event event : inEvents) {
                    inEventCount++;
                    eventCount.incrementAndGet();
                    if (inEventCount == 1) {
                        AssertJUnit.assertEquals(4L, event.getData(2));
                    } else if (inEventCount == 2) {
                        AssertJUnit.assertEquals(3L, event.getData(2));
                    } else if (inEventCount == 3) {
                        AssertJUnit.assertEquals(5L, event.getData(2));
                    } else if (inEventCount == 4) {
                        AssertJUnit.assertEquals(6L, event.getData(2));
                    } else if (inEventCount == 5) {
                        AssertJUnit.assertEquals(2L, event.getData(2));
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.8" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.91" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.91" });
        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9" });
        inputHandler.send(new Object[] { 1366335806690L, "192.10.1.10" });

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 5, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueExternalTimeBatchWindowTest9() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test9");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, 2 sec) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents == null) {
                    eventCount.incrementAndGet();
                }

                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        eventCount.incrementAndGet();
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(4L, event.getData(2));
                        } else if (inEventCount == 2) {
                            AssertJUnit.assertEquals(3L, event.getData(2));
                        } else if (inEventCount == 3) {
                            AssertJUnit.assertEquals(5L, event.getData(2));
                        } else if (inEventCount == 4) {
                            AssertJUnit.assertEquals(7L, event.getData(2));
                        } else if (inEventCount == 5) {
                            AssertJUnit.assertEquals(2L, event.getData(2));
                        }
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.8" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.91" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.92" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.92" });
        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9" });
        inputHandler.send(new Object[] { 1366335806690L, "192.10.1.10" });

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 5, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueExternalTimeBatchWindowTest10() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test10");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents == null) {
                    eventCount.incrementAndGet();
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        eventCount.incrementAndGet();
                        eventCount.incrementAndGet();
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(4L, event.getData(2));
                        } else if (inEventCount == 2) {
                            AssertJUnit.assertEquals(7L, event.getData(2));
                        }
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.8" });
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.91" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.92" });
        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9" });
        inputHandler.send(new Object[] { 1366335806690L, "192.10.1.10" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 2, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueExternalTimeBatchWindowTest11() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow Test11");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = ""
                + "define stream cseEventStream (timestamp long, symbol string, price float, volume int, ip string); "
                + "define stream twitterStream "
                + "(timestamp long, user string, tweet string, company string, ip string); ";
        String query = "" + "@info(name = 'query1') " + "from cseEventStream#window.unique:externalTimeBatch"
                + "(ip,timestamp, 1 sec, 0) "
                + "join twitterStream#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0) "
                + "on cseEventStream.symbol== twitterStream.company "
                + "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price "
                + "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);

                    eventCount.incrementAndGet();

                    if (inEvents != null) {
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
            cseEventStreamHandler.send(new Object[] { 1366335804341L, "WSO2", 55.6f, 100, "1.1.1.1" });
            cseEventStreamHandler.send(new Object[] { 1366335804341L, "WSO2", 56.6f, 100, "1.1.1.1" });
            twitterStreamHandler.send(new Object[] { 1366335804341L, "User1", "Hello World", "WSO2", "1.1.1.2" });
            twitterStreamHandler.send(new Object[] { 1366335805301L, "User2", "Hello World2", "WSO2", "1.1.1.1" });
            cseEventStreamHandler.send(new Object[] { 1366335805341L, "WSO2", 75.6f, 100, "1.1.1.1" });
            cseEventStreamHandler.send(new Object[] { 1366335806541L, "WSO2", 57.6f, 100, "1.1.1.1" });

            SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);

            Assert.assertEquals(inEventCount, 2);
            Assert.assertEquals(removeEventCount, 0);
            Assert.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test public void uniqueExternalTimeBatchWindowTest12() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow Test12");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = ""
                + "define stream cseEventStream (timestamp long, symbol string, price float, volume int, ip string); "
                + "define stream twitterStream "
                + "(timestamp long, user string, tweet string, company string, ip string); ";
        String query = "" + "@info(name = 'query1') "
                + "from cseEventStream#window.unique:externalTimeBatch(ip,timestamp, 1 sec, 0) "
                + "join twitterStream#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0) "
                + "on cseEventStream.symbol== twitterStream.company "
                + "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);

                    if (inEvents == null) {
                        eventCount.incrementAndGet();
                    }
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
            cseEventStreamHandler.send(new Object[] { 1366335804341L, "WSO2", 55.6f, 100, "1.1.1.1" });
            cseEventStreamHandler.send(new Object[] { 1366335804341L, "WSO2", 56.6f, 100, "1.1.1.1" });
            twitterStreamHandler.send(new Object[] { 1366335804341L, "User1", "Hello World", "WSO2", "1.1.1.2" });
            twitterStreamHandler.send(new Object[] { 1366335805301L, "User2", "Hello World2", "WSO2", "1.1.1.1" });
            cseEventStreamHandler.send(new Object[] { 1366335805341L, "WSO2", 75.6f, 100, "1.1.1.1" });
            cseEventStreamHandler.send(new Object[] { 1366335806541L, "WSO2", 57.6f, 100, "1.1.1.1" });

            SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);

            Assert.assertEquals(inEventCount, 2);
            Assert.assertEquals(removeEventCount, 1);
            Assert.assertTrue(eventArrived);

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test public void uniqueExternalTimeBatchWindowTest13() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test13");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, -1, false) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                for (Event event : inEvents) {
                    inEventCount++;
                    eventCount.incrementAndGet();
                    AssertJUnit.assertTrue(((Long) event.getData(0)) % 100 != 0);
                    if (inEventCount == 1) {
                        AssertJUnit.assertEquals(4L, event.getData(2));
                    } else if (inEventCount == 2) {
                        AssertJUnit.assertEquals(7L, event.getData(2));
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.8" });
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.91" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.92" });
        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9" });
        inputHandler.send(new Object[] { 1366335806690L, "192.10.1.10" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 2, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueExternalTimeBatchWindowTest14() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test14");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, -1, true) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                for (Event event : inEvents) {
                    inEventCount++;
                    eventCount.incrementAndGet();
                    AssertJUnit.assertTrue(((Long) event.getData(0)) % 100 == 0);
                    if (inEventCount == 1) {
                        AssertJUnit.assertEquals(4L, event.getData(2));
                    } else if (inEventCount == 2) {
                        AssertJUnit.assertEquals(7L, event.getData(2));
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.8" });
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.7" });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.91" });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.92" });
        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9" });
        inputHandler.send(new Object[] { 1366335806690L, "192.10.1.10" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 2, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueExternalTimeBatchWindowTest15() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindowTest test15");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string, val int) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, 2 sec) "
                + "select timestamp, ip, count() as totalCount, sum(val) as totalSum  " + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                if (inEvents == null) {
                    eventCount.incrementAndGet();
                }
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        inEventCount++;
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(3L, event.getData(2));
                            AssertJUnit.assertEquals(18L, event.getData(3));
                        } else if (inEventCount == 2) {
                            AssertJUnit.assertEquals(2L, event.getData(2));
                            AssertJUnit.assertEquals(5L, event.getData(3));
                        } else if (inEventCount == 3) {
                            AssertJUnit.assertEquals(3L, event.getData(2));
                            AssertJUnit.assertEquals(11L, event.getData(3));
                        } else if (inEventCount == 4) {
                            AssertJUnit.assertEquals(4L, event.getData(2));
                            AssertJUnit.assertEquals(20L, event.getData(3));
                        } else if (inEventCount == 5) {
                            AssertJUnit.assertEquals(2L, event.getData(2));
                            AssertJUnit.assertEquals(10L, event.getData(3));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;

            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3", 4 });
        inputHandler.send(new Object[] { 1366335804599L, "192.10.1.3", 5 });
        inputHandler.send(new Object[] { 1366335804600L, "192.10.1.5", 6 });
        inputHandler.send(new Object[] { 1366335804607L, "192.10.1.6", 7 });

        inputHandler.send(new Object[] { 1366335805599L, "192.10.1.4", 1 });
        inputHandler.send(new Object[] { 1366335805600L, "192.10.1.4", 2 });
        inputHandler.send(new Object[] { 1366335805607L, "192.10.1.6", 3 });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.6", 4 });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.8", 5 });
        Thread.sleep(2100);
        inputHandler.send(new Object[] { 1366335805606L, "192.10.1.6", 6 });
        inputHandler.send(new Object[] { 1366335805605L, "192.10.1.92", 7 });

        inputHandler.send(new Object[] { 1366335806606L, "192.10.1.9", 5 });
        inputHandler.send(new Object[] { 1366335806690L, "192.10.1.10", 5 });

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(inEventCount, 5, "In Events ");
        Assert.assertEquals(removeEventCount, 0, "Remove Events ");
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueExternalTimeBatchWindowTest17() {
        log.info("uniqueExternalTimeBatchWindowTest for second parameter should variable case ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, 'timestamp', 1 sec, 0, 2 sec) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueExternalTimeBatchWindowTest18() {
        log.info("uniqueExternalTimeBatchWindowTest for second parameter invalid type ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp string, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, 2 sec) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueExternalTimeBatchWindowTest19() {
        log.info("uniqueExternalTimeBatchWindowTest for third parameter invalid type ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, '1 sec', 0, 2 sec) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueExternalTimeBatchWindowTest20() {
        log.info("uniqueExternalTimeBatchWindowTest for fourth parameter invalid type ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, '0', 2 sec) "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueExternalTimeBatchWindowTest21() {
        log.info("uniqueExternalTimeBatchWindowTest for fifth parameter invalid type ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, '2 sec') "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueExternalTimeBatchWindowTest22() {
        log.info("uniqueExternalTimeBatchWindowTest for sixth parameter invalid type ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, 2 sec,'true') "
                + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void uniqueExternalTimeBatchWindowTest23() {
        log.info("uniqueExternalTimeBatchWindowTest for invalid number of parameters ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query =
                "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp) "
                        + "select timestamp, ip, count() as total  " + "insert into uniqueIps ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void uniqueExternalTimeBatchWindowTest24() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow test for current state and restore state");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cseEventStream = "" + "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" + "@info(name = 'query1') "
                + "from LoginEvents#window.unique:externalTimeBatch(ip,timestamp, 1 sec, 0, 6 sec) "
                + "select timestamp, ip, count() as total  " + "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents == null) {
                    eventCount.incrementAndGet();
                }
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        inEventCount++;
                        if (inEventCount == 1) {
                            AssertJUnit.assertEquals(2L, event.getData(2));
                        } else if (inEventCount == 2) {
                            AssertJUnit.assertEquals(2L, event.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { 1366335804341L, "192.10.1.3" });
        inputHandler.send(new Object[] { 1366335804342L, "192.10.1.4" });
        inputHandler.send(new Object[] { 1366335804342L, "192.10.1.4" });
        //persisting
        siddhiAppRuntime.persist();
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
        inputHandler.send(new Object[] { 1366335814341L, "192.10.1.5" });
        inputHandler.send(new Object[] { 1366335814345L, "192.10.1.6" });
        inputHandler.send(new Object[] { 1366335824341L, "192.10.1.7" });
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
