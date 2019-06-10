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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.query.processor.stream.window.WindowProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * class representing unique length batch window processor implementation.
 */

@Extension(
        name = "lengthBatch",
        namespace = "unique",
        description = "This is a batch (tumbling) window that holds a specified number of latest unique events."
                + " The unique events are determined based on the value for a specified unique key parameter."
                + " The window is updated for every window length, i.e., for the last set of events of the " +
                "specified number in a tumbling manner."
                + " When a new event arrives within the window length having the same value"
                + " for the unique key parameter as an existing event in the window,"
                + " the previous event is replaced by the new event.",

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "window.length",
                        description = "The number of events the window should tumble.",
                        type = {DataType.INT}),
        },
        examples = {
                @Example(
                        syntax = "define window CseEventWindow (symbol string, price float, volume int)\n\n " +
                                "from CseEventStream#window.unique:lengthBatch(symbol, 10)\n" +
                                "select symbol, price, volume\n" +
                                "insert expired events into OutputStream ;",
                        description = "In this query, the window at any give time holds"
                                + " the last 10 unique events from the 'CseEventStream' stream."
                                + " Each of the 10 events within the window at a given time has"
                                + " a unique value for the symbol attribute. If a new event has the same value"
                                + " for the symbol attribute as an existing event within the window length,"
                                + " the existing event expires and it is replaced by the new event."
                                + " The query returns expired individual events as well as expired batches"
                                + " of events to the 'OutputStream' stream."
                )
        }
)

public class UniqueLengthBatchWindowProcessor extends WindowProcessor implements FindableProcessor {

    private int windowLength;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ComplexEventChunk<StreamEvent> eventsToBeExpired = null;
    private SiddhiAppContext siddhiAppContext;
    private StreamEvent resetEvent = null;
    private ExpressionExecutor uniqueKeyExpressionExecutor;
    private Map<Object, StreamEvent> uniqueEventMap = new HashMap<>();


    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        boolean b, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.eventsToBeExpired = new ComplexEventChunk<>(false);
        if (attributeExpressionExecutors.length == 2) {
            this.uniqueKeyExpressionExecutor = attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    this.windowLength = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue());
                } else {
                    throw new SiddhiAppValidationException(
                            "Unique Length Batch window's Length parameter should be INT, but found "
                                    + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Unique Length Batch window should have constant "
                        + "for Length parameter but found a dynamic attribute " + attributeExpressionExecutors[1]
                        .getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Unique Length batch window should only have two parameters, " + "but found "
                            + attributeExpressionExecutors.length + " input attributes");
        }

    }


    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                addUniqueEvent(uniqueEventMap, uniqueKeyExpressionExecutor, clonedStreamEvent);
                if (uniqueEventMap.size() == windowLength) {
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
                        outputStreamEventChunk.add(eventsToBeExpired.getFirst());
                    }
                    eventsToBeExpired.clear();
                    if (currentEventChunk.getFirst() != null) {
                        // add reset event in front of current events
                        outputStreamEventChunk.add(resetEvent);
                        currentEventChunk.reset();
                        while (currentEventChunk.hasNext()) {
                            StreamEvent toExpireEvent = currentEventChunk.next();
                            StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(toExpireEvent);
                            eventClonedForMap.setType(StreamEvent.Type.EXPIRED);
                            eventsToBeExpired.add(eventClonedForMap);
                        }
                        resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                        resetEvent.setType(ComplexEvent.Type.RESET);
                        outputStreamEventChunk.add(currentEventChunk.getFirst());
                    }
                    currentEventChunk.clear();
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }
    }

    protected void addUniqueEvent(Map<Object, StreamEvent> uniqueEventMap, ExpressionExecutor uniqueKey,
                                  StreamEvent clonedStreamEvent) {
        uniqueEventMap.put(uniqueKey.execute(clonedStreamEvent), clonedStreamEvent);
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
            map.put("count", count);
            map.put("resetEvent", resetEvent);
            return map;
        } else {
            Map<String, Object> map = new HashMap<>();
            map.put("currentEventChunk", currentEventChunk.getFirst());
            map.put("count", count);
            map.put("resetEvent", resetEvent);
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        if (map.size() > 3) {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) map.get("currentEventChunk"));
            eventsToBeExpired.clear();
            eventsToBeExpired.add((StreamEvent) map.get("eventsToBeExpired"));
            count = (Integer) map.get("count");
            resetEvent = (StreamEvent) map.get("resetEvent");
        } else {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) map.get("currentEventChunk"));
            count = (Integer) map.get("count");
            resetEvent = (StreamEvent) map.get("resetEvent");
        }
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, uniqueEventMap.values(), streamEventCloner);
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
        return OperatorParser.constructOperator(uniqueEventMap.values(), expression, matchingMetaInfoHolder,
                siddhiAppContext, list, map, queryName);
    }
}
