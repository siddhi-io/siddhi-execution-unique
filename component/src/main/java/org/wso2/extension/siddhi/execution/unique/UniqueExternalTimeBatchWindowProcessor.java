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

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class representing Unique External TimeBatch Window Processor Implementation.
 */


@Extension(
        name = "externalTimeBatch",
        namespace = "unique",
        description = "This is a batch (tumbling) time window that is determined based on an external time, i.e., " +
                "time stamps that are specified via an attribute in the events."
                + " It holds the latest unique events that arrived during the last window time period."
                + " The unique events are determined based on the value for a specified unique key parameter."
                + " When a new event arrives within the time window with a value for the unique key parameter"
                + " that is the same as that of an existing event in the window,"
                + " the existing event expires and it is replaced by the new event." ,

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "time.stamp",
                        description = " The time which the window determines as the current time and acts upon."
                                + " The value of this parameter should be monotonically increasing.",
                        type = { DataType.LONG}),
                @Parameter(name = "window.time",
                        description = "The sliding time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG}),
                @Parameter(name = "start.time",
                        description = "This specifies an offset in milliseconds in order to start the" +
                                " window at a time different to the standard time.",
                        defaultValue = "Timestamp of first event",
                        type = {DataType.INT}, optional = true),
                @Parameter(name = "time.out",
                        description = "Time to wait for arrival of a new event, before flushing " +
                                "and returning the output for events belonging to a specific batch.",
                        type = {DataType.INT, DataType.LONG},
                        optional = true,
                        defaultValue = "The system waits till an event from the next batch arrives to flush " +
                                "the current batch") ,
                @Parameter(name = "replace.time.stamp.with.batch.end.time",
                        description = "Replaces the 'timestamp' value with the corresponding batch end time stamp." ,
                        type = {DataType.INT, DataType.LONG},
                        optional = true,
                        defaultValue = "false")
        },
        examples = {
                @Example(
                        syntax = "define stream LoginEvents (timestamp long, ip string) ;\n" +
                                "from LoginEvents#window.unique:externalTimeBatch(ip, timestamp, 1 sec, 0, 2 sec) \n" +
                                "select timestamp, ip, count() as total\n" +
                                "insert into UniqueIps ;",

                        description = "In this query, the window holds the latest unique events"
                                + " that arrive from the 'LoginEvent' stream during each second."
                                + " The latest events are determined based on the external time stamp."
                                + " At a given time, all the events held in the window have unique"
                                + " values for the 'ip' and monotonically increasing values for 'timestamp' attributes."
                                + " The events in the window are inserted into the 'UniqueIps' output stream."
                                + " The system waits for 2 seconds"
                                + " for the arrival of a new event before flushing the current batch."
                )
        }
)

public class UniqueExternalTimeBatchWindowProcessor extends WindowProcessor
        implements SchedulingProcessor, FindableProcessor {
    private Map<Object, StreamEvent> currentEvents = new LinkedHashMap<Object, StreamEvent>();
    private Map<Object, StreamEvent> expiredEvents = null;
    private volatile StreamEvent resetEvent = null;
    private VariableExpressionExecutor timestampExpressionExecutor;
    private long timeToKeep;
    private long endTime = -1;
    private long startTime = 0;
    private boolean isStartTimeEnabled = false;
    private long schedulerTimeout = 0;
    private Scheduler scheduler;
    private long lastScheduledTime;
    private long lastCurrentEventTime;
    private boolean flushed = false;
    private boolean storeExpiredEvents = false;
    private ExpressionExecutor uniqueExpressionExecutor;
    private boolean replaceTimestampWithBatchEndTime = false;
    private boolean outputExpectsExpiredEvents;

    @Override protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
            boolean outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {

        if (outputExpectsExpiredEvents) {
            this.expiredEvents = new LinkedHashMap<Object, StreamEvent>();
            this.storeExpiredEvents = true;
        }
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        if (attributeExpressionExecutors.length >= 3 && attributeExpressionExecutors.length <= 6) {

            uniqueExpressionExecutor = attributeExpressionExecutors[0];

            if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
                throw new SiddhiAppValidationException(
                        "ExternalTime window's 2nd parameter timestamp should be a variable, but found "
                                + attributeExpressionExecutors[1].getClass());
            }
            if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.LONG) {
                throw new SiddhiAppValidationException(
                        "ExternalTime window's 2nd parameter timestamp should be type long, but found "
                                + attributeExpressionExecutors[1].getReturnType());
            }
            timestampExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[1];

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                timeToKeep = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                timeToKeep = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppValidationException(
                        "ExternalTimeBatch window's 3rd parameter windowTime should be either int or long, but found "
                                + attributeExpressionExecutors[2].getReturnType());
            }

            if (attributeExpressionExecutors.length >= 4) {
                isStartTimeEnabled = true;
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                    startTime = Integer.parseInt(
                            String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue()));
                } else if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                    startTime = Long.parseLong(
                            String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue()));
                } else {
                    throw new SiddhiAppValidationException(
                            "ExternalTimeBatch window's 4th parameter startTime should be "
                                    + "either int or long, but found " + attributeExpressionExecutors[3]
                                    .getReturnType());
                }
            }

            if (attributeExpressionExecutors.length >= 5) {
                if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.INT) {
                    schedulerTimeout = Integer.parseInt(
                            String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue()));
                } else if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.LONG) {
                    schedulerTimeout = Long.parseLong(
                            String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue()));
                } else {
                    throw new SiddhiAppValidationException(
                            "ExternalTimeBatch window's 5th parameter timeout should be either int or long, but found "
                                    + attributeExpressionExecutors[4].getReturnType());
                }
            }

            if (attributeExpressionExecutors.length == 6) {
                if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.BOOL) {
                    replaceTimestampWithBatchEndTime = Boolean.parseBoolean(
                            String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[5]).getValue()));
                } else {
                    throw new SiddhiAppValidationException("ExternalTimeBatch window's 6th parameter "
                            + "replaceTimestampWithBatchEndTime should be bool, but found "
                            + attributeExpressionExecutors[5].getReturnType());
                }
            }
        } else {
            throw new SiddhiAppValidationException("ExternalTimeBatch window should only have three to six parameters "
                    + "(<variable> uniqueAttribute, <long> timestamp, "
                    + "<int|long|time> windowTime, <long> startTime, <int|long|time> timeout, "
                    + "<bool> replaceTimestampWithBatchEndTime), but found " + attributeExpressionExecutors.length
                    + " input attributes");
        }
        if (schedulerTimeout > 0) {
            if (expiredEvents == null) {
                this.expiredEvents = new LinkedHashMap<Object, StreamEvent>();
            }
        }

    }

    @Override protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner) {

        // event incoming trigger process. No events means no action
        if (streamEventChunk.getFirst() == null) {
            return;
        }

        List<ComplexEventChunk<StreamEvent>> complexEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            initTiming(streamEventChunk.getFirst());

            StreamEvent nextStreamEvent = streamEventChunk.getFirst();
            while (nextStreamEvent != null) {

                StreamEvent currStreamEvent = nextStreamEvent;
                nextStreamEvent = nextStreamEvent.getNext();

                if (currStreamEvent.getType() == ComplexEvent.Type.TIMER) {
                    if (lastScheduledTime <= currStreamEvent.getTimestamp()) {
                        // implies that there have not been any more events after this schedule has been done.
                        if (!flushed) {
                            flushToOutputChunk(streamEventCloner, complexEventChunks, lastCurrentEventTime, true);
                            flushed = true;
                        } else {
                            if (currentEvents.size() > 0) {
                                appendToOutputChunk(streamEventCloner, complexEventChunks, lastCurrentEventTime, true);
                            }
                        }

                        // rescheduling to emit the current batch after expiring it if no further events arrive.
                        lastScheduledTime = siddhiAppContext.getTimestampGenerator().currentTime() + schedulerTimeout;
                        if (scheduler != null) {
                            scheduler.notifyAt(lastScheduledTime);
                        }
                    }
                    continue;
                } else if (currStreamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }

                long currentEventTime = (Long) timestampExpressionExecutor.execute(currStreamEvent);
                if (lastCurrentEventTime < currentEventTime) {
                    lastCurrentEventTime = currentEventTime;
                }

                if (currentEventTime < endTime) {
                    cloneAppend(streamEventCloner, currStreamEvent);
                } else {
                    if (flushed) {
                        appendToOutputChunk(streamEventCloner, complexEventChunks, lastCurrentEventTime, false);
                        flushed = false;
                    } else {
                        flushToOutputChunk(streamEventCloner, complexEventChunks, lastCurrentEventTime, false);
                    }
                    // update timestamp, call next processor
                    endTime = findEndTime(lastCurrentEventTime, startTime, timeToKeep);
                    cloneAppend(streamEventCloner, currStreamEvent);
                    // triggering the last batch expiration.
                    if (schedulerTimeout > 0) {
                        lastScheduledTime = siddhiAppContext.getTimestampGenerator().currentTime() + schedulerTimeout;
                        scheduler.notifyAt(lastScheduledTime);
                    }
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> complexEventChunk : complexEventChunks) {
            nextProcessor.process(complexEventChunk);
        }
    }

    private void initTiming(StreamEvent firstStreamEvent) {
        // for window beginning, if window is empty, set lastSendTime to incomingChunk first.
        if (endTime < 0) {
            if (isStartTimeEnabled) {
                endTime = findEndTime((Long) timestampExpressionExecutor.execute(firstStreamEvent), startTime,
                        timeToKeep);
            } else {
                startTime = (Long) timestampExpressionExecutor.execute(firstStreamEvent);
                endTime = startTime + timeToKeep;
            }
            if (schedulerTimeout > 0) {
                lastScheduledTime = siddhiAppContext.getTimestampGenerator().currentTime() + schedulerTimeout;
                if (scheduler != null) {
                    scheduler.notifyAt(lastScheduledTime);
                }
            }
        }
    }

    private void flushToOutputChunk(StreamEventCloner streamEventCloner,
            List<ComplexEventChunk<StreamEvent>> complexEventChunks, long currentTime, boolean preserveCurrentEvents) {

        ComplexEventChunk<StreamEvent> newEventChunk = new ComplexEventChunk<StreamEvent>(true);
        if (outputExpectsExpiredEvents) {
            if (expiredEvents.size() > 0) {
                // mark the timestamp for the expiredType event
                for (StreamEvent expiredEvent : expiredEvents.values()) {
                    expiredEvent.setTimestamp(currentTime);
                    // add expired event to newEventChunk.
                    newEventChunk.add(expiredEvent);
                }
            }
        }
        if (expiredEvents != null) {
            expiredEvents.clear();
        }

        if (currentEvents.size() > 0) {

            // add reset event in front of current events
            resetEvent.setTimestamp(currentTime);
            newEventChunk.add(resetEvent);
            resetEvent = null;

            // move to expired events
            for (Map.Entry<Object, StreamEvent> currentEventEntry : currentEvents.entrySet()) {
                if (preserveCurrentEvents || storeExpiredEvents) {
                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEventEntry.getValue());
                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                    expiredEvents.put(currentEventEntry.getKey(), toExpireEvent);
                }
                // add current event to next processor
                newEventChunk.add(currentEventEntry.getValue());
            }

        }
        currentEvents.clear();

        if (newEventChunk.getFirst() != null) {
            complexEventChunks.add(newEventChunk);
        }
    }

    private void appendToOutputChunk(StreamEventCloner streamEventCloner,
            List<ComplexEventChunk<StreamEvent>> complexEventChunks, long currentTime, boolean preserveCurrentEvents) {
        ComplexEventChunk<StreamEvent> newEventChunk = new ComplexEventChunk<StreamEvent>(true);
        Map<Object, StreamEvent> sentEvents = new LinkedHashMap<Object, StreamEvent>();

        if (currentEvents.size() > 0) {

            if (expiredEvents.size() > 0) {
                // mark the timestamp for the expiredType event
                for (Map.Entry<Object, StreamEvent> expiredEventEntry : expiredEvents.entrySet()) {
                    if (outputExpectsExpiredEvents) {
                        // add expired event to newEventChunk.
                        StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(expiredEventEntry.getValue());
                        toExpireEvent.setTimestamp(currentTime);
                        newEventChunk.add(toExpireEvent);
                    }

                    StreamEvent toSendEvent = streamEventCloner.copyStreamEvent(expiredEventEntry.getValue());
                    toSendEvent.setType(ComplexEvent.Type.CURRENT);
                    sentEvents.put(expiredEventEntry.getKey(), toSendEvent);
                }
            }

            // add reset event in front of current events
            StreamEvent toResetEvent = streamEventCloner.copyStreamEvent(resetEvent);
            toResetEvent.setTimestamp(currentTime);
            newEventChunk.add(toResetEvent);

            for (Map.Entry<Object, StreamEvent> currentEventEntry : currentEvents.entrySet()) {
                // move to expired events
                if (preserveCurrentEvents || storeExpiredEvents) {
                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEventEntry.getValue());
                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                    expiredEvents.put(currentEventEntry.getKey(), toExpireEvent);
                }
                sentEvents.put(currentEventEntry.getKey(), currentEventEntry.getValue());
            }

            for (StreamEvent sentEventEntry : sentEvents.values()) {
                newEventChunk.add(sentEventEntry);
            }
        }
        currentEvents.clear();

        if (newEventChunk.getFirst() != null) {
            complexEventChunks.add(newEventChunk);
        }
    }

    private long findEndTime(long currentTime, long startTime, long timeToKeep) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - startTime) % timeToKeep;
        return (currentTime + (timeToKeep - elapsedTimeSinceLastEmit));
    }

    private void cloneAppend(StreamEventCloner streamEventCloner, StreamEvent currStreamEvent) {
        StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(currStreamEvent);
        if (replaceTimestampWithBatchEndTime) {
            clonedStreamEvent.setAttribute(endTime, timestampExpressionExecutor.getPosition());
        }
        currentEvents.put(uniqueExpressionExecutor.execute(clonedStreamEvent), clonedStreamEvent);
        if (resetEvent == null) {
            resetEvent = streamEventCloner.copyStreamEvent(currStreamEvent);
            resetEvent.setType(ComplexEvent.Type.RESET);
        }
    }

    public void start() {
        //Do nothing
    }

    public void stop() {
        //Do nothing
    }

    public synchronized Map<String, Object> currentState() {
        Map<String, Object> map = new HashMap<>();
        map.put("currentEvents", currentEvents);
        map.put("expiredEvents", expiredEvents);
        map.put("resetEvent", resetEvent);
        map.put("endTime", endTime);
        map.put("startTime", startTime);
        map.put("lastScheduledTime", lastScheduledTime);
        map.put("lastCurrentEventTime", lastCurrentEventTime);
        map.put("flushed", flushed);

        return map;
    }

    public synchronized void restoreState(Map<String, Object> map) {
        currentEvents = (Map<Object, StreamEvent>) map.get("currentEvents");
        if (map.get("expiredEvents") != null) {
            expiredEvents = (Map<Object, StreamEvent>) map.get("expiredEvents");
        } else {
            if (outputExpectsExpiredEvents) {
                this.expiredEvents = new LinkedHashMap<Object, StreamEvent>();
            }
            if (schedulerTimeout > 0) {
                this.expiredEvents = new LinkedHashMap<Object, StreamEvent>();
            }
        }
        resetEvent = (StreamEvent) map.get("resetEvent");
        endTime = (Long) map.get("endTime");
        startTime = (Long) map.get("startTime");
        lastScheduledTime = (Long) map.get("lastScheduledTime");
        lastCurrentEventTime = (Long) map.get("lastCurrentEventTime");
        flushed = (Boolean) map.get("flushed");
    }

    @Override public synchronized Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override public synchronized void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override public synchronized StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, expiredEvents, streamEventCloner);
        } else {
            return null;
        }
    }

    @Override public synchronized CompiledCondition compileCondition(Expression expression,
            MatchingMetaInfoHolder matchingMetaInfoHolder, SiddhiAppContext siddhiAppContext,
            List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, Table> tableMap, String s) {
        if (expiredEvents == null) {
            expiredEvents = new LinkedHashMap<Object, StreamEvent>();
            storeExpiredEvents = true;
        }
        return OperatorParser.constructOperator(expiredEvents, expression, matchingMetaInfoHolder, siddhiAppContext,
                variableExpressionExecutors, tableMap, this.queryName);
    }
}
