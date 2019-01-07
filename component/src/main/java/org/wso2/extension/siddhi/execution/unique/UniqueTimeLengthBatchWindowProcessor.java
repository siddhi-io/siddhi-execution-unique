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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * class representing Unique Time Length Batch window processor implementation.
 */

@Extension(
        name = "timeLengthBatch",
        namespace = "unique",
        description = "This is a batch or tumbling time length window that is updated "
                + "with the latest events based on a unique key parameter. The window tumbles "
                + "upon the elapse of the time window, or when a number of unique events have arrived."
                + " If a new event that arrives within the period of the window "
                + "has a value for the key parameter which matches the value of an existing event, "
                + "the existing event expires and it is replaced by the new event. ",
        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),

                @Parameter(name = "window.time",
                        description = "The sliding time period for which the window should hold the events.",
                        type = {DataType.INT, DataType.LONG}),

                @Parameter(name = "start.time",
                        description = "This specifies an offset in milliseconds in order to start the" +
                                " window at a time different to the standard time.",
                        type = {DataType.INT, DataType.LONG},
                        optional = true,
                        defaultValue = "Timestamp of first event"),

                @Parameter(name = "window.length",
                        description = "The number of events the window should tumble.",
                        type = {DataType.INT})
        },
        examples = {
                @Example(
                        syntax = "define stream CseEventStream (symbol string, price float, volume int)\n\n" +
                                "from CseEventStream#window.unique:timeLengthBatch(symbol, 1 sec, 20)\n" +
                                "select symbol, price, volume\n" +
                                "insert all events into OutputStream ;",

                        description = "This window holds the latest unique events that arrive from the 'CseEventStream'"
                                + " at a given time, and returns all the events to the 'OutputStream' stream. "
                                + "It is updated every second based on the latest values for the 'symbol' attribute."
                )
        }
)

public class UniqueTimeLengthBatchWindowProcessor extends WindowProcessor implements SchedulingProcessor,
        FindableProcessor {

    private long timeInMilliSeconds;
    private long length;
    private long nextEmitTime = -1;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<>(false);
    private ComplexEventChunk<StreamEvent> eventsToBeExpired = null;
    private Map<Object, StreamEvent> uniqueEventMap = new HashMap<>();
    private StreamEvent resetEvent = null;
    private Scheduler scheduler;
    private SiddhiAppContext siddhiAppContext;
    private boolean isStartTimeEnabled = false;
    private long startTime = 0;
    private ExpressionExecutor uniqueKeyExpressionExecutor;
    private boolean eventSent = false;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        boolean outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.eventsToBeExpired = new ComplexEventChunk<>(false);
        if (attributeExpressionExecutors.length == 3) {
            this.uniqueKeyExpressionExecutor = attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                } else {
                    throw new SiddhiAppValidationException(
                            "Unique Time Length Batch window's parameter time should be either int, long or time, " +
                                    "but found " + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Unique Time Length Batch window should have constant "
                        + "for time parameter but found a dynamic attribute " + attributeExpressionExecutors[1]
                        .getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    length = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                            .getValue();
                } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                    length = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                            .getValue();
                } else {
                    throw new SiddhiAppValidationException(
                            "Unique Time Length Batch window's parameter " + "length should be either"
                                    + "int or long, but found " + attributeExpressionExecutors[2].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Unique Time Length Batch window should have constant "
                        + "for length parameter but found a dynamic attribute " + attributeExpressionExecutors[2]
                        .getClass().getCanonicalName());
            }
        } else if (attributeExpressionExecutors.length == 4) {
            this.uniqueKeyExpressionExecutor = attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                } else {
                    throw new SiddhiAppValidationException(
                            "UniqueTimeLengthBatch window's parameter time should be either" +
                                    " int or long, but found "
                                    + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Unique Time Length Batch window should have constant "
                        + "for time parameter but found a dynamic attribute " + attributeExpressionExecutors[1]
                        .getClass().getCanonicalName());
            }
            // isStartTimeEnabled used to set start time
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    isStartTimeEnabled = true;
                    startTime = Integer.parseInt(
                            String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
                } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                    isStartTimeEnabled = true;
                    startTime = Long.parseLong(
                            String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
                } else {
                    throw new SiddhiAppValidationException("Expected either int or long type for " +
                            "UniqueTimeLengthBatch window's start time parameter, but found "
                            + attributeExpressionExecutors[2].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Unique Time Length Batch window should have constant "
                        + "for time parameter but found a dynamic attribute " + attributeExpressionExecutors[2]
                        .getReturnType());
            }
            if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                    length = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[3])
                            .getValue();
                } else if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                    length = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[3])
                            .getValue();
                } else {
                    throw new SiddhiAppValidationException(
                            "Unique Time Length Batch window's parameter " + "length should be either"
                                    + "int or long, but found " + attributeExpressionExecutors[3].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Unique Time Length Batch window should have constant "
                        + "for length parameter but found a dynamic attribute " + attributeExpressionExecutors[3]
                        .getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Unique Time Length Batch window should " + "only have three or four parameters. " + "but found "
                            + attributeExpressionExecutors.length + " input attributes");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            if (nextEmitTime == -1) {
                if (isStartTimeEnabled) {
                    nextEmitTime = getNextEmitTime(currentTime);
                } else {
                    nextEmitTime = currentTime + timeInMilliSeconds;
                }
                if (scheduler != null) {
                    scheduler.notifyAt(nextEmitTime);
                }
            }
            boolean sendEventsByTime = false;
            boolean sendEventsByLength = false;
            if (currentTime >= nextEmitTime) {
                nextEmitTime += timeInMilliSeconds;
                if (scheduler != null) {
                    scheduler.notifyAt(nextEmitTime);
                }
                if (eventSent) { // reset on next batch
                    eventSent = false;
                    streamEventChunk.clear();
                    return;
                }
                sendEventsByTime = true;
            }
            if (eventSent) { //skip events till next batch
                streamEventChunk.clear();
                return;
            }
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                addUniqueEvent(uniqueEventMap, uniqueKeyExpressionExecutor, clonedStreamEvent);
                if (uniqueEventMap.size() == length) {
                    sendEventsByLength = true; // emitting batch based on length
                    break;
                }
            }
            streamEventChunk.clear();
            if (sendEventsByTime || sendEventsByLength) {
                sendEvents(streamEventChunk, streamEventCloner, currentTime);
            }
            if (sendEventsByLength) {
                eventSent = true; // making events to skip till next time batch
            }
        }
        if (streamEventChunk.getFirst() != null) {
            streamEventChunk.setBatch(true);
            nextProcessor.process(streamEventChunk);
            streamEventChunk.setBatch(false);
        }
    }

    private void sendEvents(ComplexEventChunk<StreamEvent> streamEventChunk, StreamEventCloner streamEventCloner,
                            long currentTime) {
        for (StreamEvent event : uniqueEventMap.values()) {
            event.setTimestamp(currentTime);
            currentEventChunk.add(event);
        }
        uniqueEventMap.clear();
        if (eventsToBeExpired.getFirst() != null) {
            while (eventsToBeExpired.hasNext()) {
                StreamEvent expiredEvent = eventsToBeExpired.next();
                expiredEvent.setTimestamp(currentTime);
            }
            streamEventChunk.add(eventsToBeExpired.getFirst());
        }
        eventsToBeExpired.clear();
        if (currentEventChunk.getFirst() != null) {
            // add reset event in front of current events
            streamEventChunk.add(resetEvent);
            currentEventChunk.reset();
            while (currentEventChunk.hasNext()) {
                StreamEvent streamEvent = currentEventChunk.next();
                StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(streamEvent);
                eventClonedForMap.setType(StreamEvent.Type.EXPIRED);
                this.eventsToBeExpired.add(eventClonedForMap);
            }
            if (currentEventChunk.getFirst() != null) {
                resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                resetEvent.setType(ComplexEvent.Type.RESET);
                streamEventChunk.add(currentEventChunk.getFirst());
            }
        }
        currentEventChunk.clear();
    }


    @Override
    public synchronized void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public synchronized Scheduler getScheduler() {
        return scheduler;
    }

    protected void addUniqueEvent(Map<Object, StreamEvent> uniqueEventMap,
                                  ExpressionExecutor uniqueKeyExpressionExecutor,
                                  StreamEvent clonedStreamEvent) {
        uniqueEventMap.put(uniqueKeyExpressionExecutor.execute(clonedStreamEvent), clonedStreamEvent);
    }

    /**
     * returns the next emission time based on system clock round time values.
     *
     * @param currentTime the current time.
     * @return next emit time
     */
    private long getNextEmitTime(long currentTime) {
        long elapsedTimeSinceLastEmit = (currentTime - startTime) % timeInMilliSeconds;
        return currentTime + (timeInMilliSeconds - elapsedTimeSinceLastEmit);
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Map<String, Object> currentState() {
        if (eventsToBeExpired != null) {
            Map<String, Object> map = new HashMap<>();
            map.put("currentEventChunk", currentEventChunk.getFirst());
            map.put("eventsToBeExpired", eventsToBeExpired.getFirst());
            map.put("resetEvent", resetEvent);
            return map;
        } else {
            Map<String, Object> map = new HashMap<>();
            map.put("currentEventChunk", currentEventChunk.getFirst());
            map.put("resetEvent", resetEvent);
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        if (map.size() > 2) {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) map.get("currentEventChunk"));
            eventsToBeExpired.clear();
            eventsToBeExpired.add((StreamEvent) map.get("eventsToBeExpired"));
            resetEvent = (StreamEvent) map.get("resetEvent");
        } else {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) map.get("currentEventChunk"));
            resetEvent = (StreamEvent) map.get("resetEvent");
        }
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, eventsToBeExpired, streamEventCloner);
        } else {
            return null;
        }
    }

    @Override
    public CompiledCondition compileCondition(Expression expression,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              SiddhiAppContext siddhiAppContext,
                                              List<VariableExpressionExecutor> list, Map<String, Table> map,
                                              String queryName) {
        return OperatorParser
                .constructOperator(eventsToBeExpired, expression, matchingMetaInfoHolder, siddhiAppContext, list, map,
                        queryName);
    }
}
