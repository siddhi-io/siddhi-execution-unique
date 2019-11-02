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
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.query.processor.stream.window.WindowProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class representing unique length window processor implementation.
 */

@Extension(
        name = "length",
        namespace = "unique",
        description = "This is a sliding length window that holds the events of the latest window length "
                + "with the unique key"
                + " and gets updated for the expiry and arrival of each event."
                + " When a new event arrives with the key that is already there in the window, "
                + "then the previous event expires and new event is kept within the window.",
        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE, DataType.STRING},
                        dynamic = true),
                @Parameter(name = "window.length",
                        description = "The number of events that should be "
                                + "included in a sliding length window.",
                        type = {DataType.INT})
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"unique.key", "window.length"})
        },
        examples = @Example(
                syntax = "define stream CseEventStream (symbol string, price float, volume int)\n\n" +
                        "from CseEventStream#window.unique:length(symbol,10)\n" +
                        "select symbol, price, volume\n" +
                        "insert all events into OutputStream;",

                description = "In this configuration, the window holds the latest 10 unique events."
                        + " The latest events are selected based on the symbol attribute. "
                        + "If the 'CseEventStream' receives an event for which the value for the symbol attribute "
                        + "is the same as that of an existing event in the window,"
                        + " the existing event is replaced by the new event. "
                        + "All the events are returned to the 'OutputStream' event stream "
                        + "once an event expires or is added to the window."
        )
)
public class UniqueLengthWindowProcessor extends WindowProcessor<UniqueLengthWindowProcessor.WindowState>
        implements FindableProcessor {
    private ExpressionExecutor uniqueKeyExpressionExecutor;
    private int length;
    private SiddhiAppContext siddhiAppContext;
    private StreamEventCloner streamEventCloner;


    @Override
    protected StateFactory<WindowState> init(MetaStreamEvent metaStreamEvent,
                                             AbstractDefinition inputDefinition,
                                             ExpressionExecutor[] attributeExpressionExecutors,
                                             ConfigReader configReader,
                                             StreamEventClonerHolder streamEventClonerHolder,
                                             boolean outputExpectsExpiredEvents,
                                             boolean findToBeExecuted,
                                             SiddhiQueryContext siddhiQueryContext) {
        siddhiAppContext = siddhiQueryContext.getSiddhiAppContext();
        if (attributeExpressionExecutors.length == 2) {
            uniqueKeyExpressionExecutor = attributeExpressionExecutors[0];
            length = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        } else {
            throw new SiddhiAppValidationException("Unique Length window should only have two parameters "
                    + "(<string|int|bool|long|double|float> attribute, <int> windowLength), but found "
                    + attributeExpressionExecutors.length + " input attributes");
        }
        return () -> new WindowState(new ComplexEventChunk<>(false));
    }

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                     WindowState state) {
        this.streamEventCloner = streamEventCloner;
        synchronized (state) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                streamEvent.setNext(null);
                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);
                StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(clonedEvent);
                StreamEvent oldEvent = state.map.put(generateKey(eventClonedForMap), eventClonedForMap);
                if (oldEvent == null) {
                    state.count++;
                }
                if ((state.count <= length) && (oldEvent == null)) {
                    state.expiredEventChunk.add(clonedEvent);
                } else {
                    if (oldEvent != null) {
                        while (state.expiredEventChunk.hasNext()) {
                            StreamEvent firstEventExpired = state.expiredEventChunk.next();
                            if (firstEventExpired.equals(oldEvent)) {
                                state.expiredEventChunk.remove();
                            }
                        }
                        state.expiredEventChunk.add(clonedEvent);
                        streamEventChunk.insertBeforeCurrent(oldEvent);
                        oldEvent.setTimestamp(currentTime);
                    } else {
                        StreamEvent firstEvent = state.expiredEventChunk.poll();
                        if (firstEvent != null) {
                            firstEvent.setTimestamp(currentTime);
                            streamEventChunk.insertBeforeCurrent(firstEvent);
                            state.expiredEventChunk.add(clonedEvent);
                        } else {
                            streamEventChunk.insertBeforeCurrent(clonedEvent);
                        }
                    }
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
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
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        WindowState state = stateHolder.getState();
        StreamEvent streamEvent = null;
        try {
            if (compiledCondition instanceof Operator) {
                streamEvent =  ((Operator) compiledCondition).find(matchingEvent, state.expiredEventChunk,
                        streamEventCloner);
            }
        } finally {
            stateHolder.returnState(state);
        }
        return streamEvent;
    }

    private String generateKey(StreamEvent event) {
        return uniqueKeyExpressionExecutor.execute(event).toString();
    }

    @Override
    public CompiledCondition compileCondition(Expression expression, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        WindowState state = stateHolder.getState();
        CompiledCondition compiledCondition;
        try {
            compiledCondition = OperatorParser.constructOperator(state.expiredEventChunk, expression,
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
        } finally {
            stateHolder.returnState(state);
        }
        return compiledCondition;
    }

    class WindowState extends State {
        private ConcurrentHashMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();
        private ComplexEventChunk<StreamEvent> expiredEventChunk;
        private int count = 0;

        WindowState(ComplexEventChunk<StreamEvent> expiredEventChunk) {
            this.expiredEventChunk = expiredEventChunk;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("expiredEventChunk", expiredEventChunk.getFirst());
            map.put("count", count);
            map.put("map", this.map);
            return map;
        }

        @Override
        public void restore(Map<String, Object> state) {
            expiredEventChunk.clear();
            expiredEventChunk.add((StreamEvent) state.get("expiredEventChunk"));
            count = (Integer) state.get("count");
            this.map = (ConcurrentHashMap) state.get("map");

        }
    }
}
