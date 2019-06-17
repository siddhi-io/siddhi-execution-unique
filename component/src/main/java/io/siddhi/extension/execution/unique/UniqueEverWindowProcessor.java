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

package io.siddhi.extension.execution.unique;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
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
import io.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.singletonMap;

/**
 * This is Unique Ever Window Processor implementation.
 */

@Extension(
        name = "ever",
        namespace = "unique",
        description = "This is a window that is updated with the latest events based on a unique key parameter."
                + " When a new event arrives with the same value for the unique key parameter"
                + " as the existing event, the existing event expires, "
                + "and is replaced with the latest one.",

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness."
                                + "If multiple attributes need to be checked, we can specify them "
                                + "as a comma-separated list.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),
        },
        examples = {
                @Example(
                        syntax = "define stream LoginEvents (timeStamp long, ip string) ;\n" +
                                "from LoginEvents#window.unique:ever(ip)\n" +
                                "select count(ip) as ipCount, ip \n" +
                                "insert all events into UniqueIps  ;",

                        description = "The above query determines the latest events that have arrived "
                                + "from the 'LoginEvents' stream, based on the 'ip' attribute. "
                                + "At a given time, all the events held in the window should have a unique value "
                                + "for the ip attribute. All the processed events are directed "
                                + "to the 'UniqueIps' output stream with 'ip' and 'ipCount' attributes."

                ),
                @Example(
                        syntax = "define stream LoginEvents (timeStamp long, ip string , id string) ;\n" +
                                "from LoginEvents#window.unique:ever(ip, id)\n" +
                                "select count(ip) as ipCount, ip , id \n" +
                                "insert expired events into UniqueIps  ;",

                        description = "This query determines the latest events to be included in the window "
                                + "based on the ip and id attributes. When the 'LoginEvents' event stream receives"
                                + " a new event of which the combination of values for the ip and id attributes "
                                + "matches that of an existing event in the window, the existing event expires"
                                + " and it is replaced with the new event. The expired events "
                                + "which have been expired"
                                + " as a result of being replaced by a newer event"
                                + " are directed to the 'uniqueIps' output stream."
                )
        }
)

public class UniqueEverWindowProcessor extends WindowProcessor<UniqueEverWindowProcessor.WindowState>
        implements FindableProcessor {
    private ExpressionExecutor[] expressionExecutors;
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
        expressionExecutors = attributeExpressionExecutors;
        siddhiAppContext = siddhiQueryContext.getSiddhiAppContext();
        return WindowState::new;
    }

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                     WindowState state) {

        this.streamEventCloner = streamEventCloner;
        synchronized (state) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();

            StreamEvent streamEvent = streamEventChunk.getFirst();
            streamEventChunk.clear();
            while (streamEvent != null) {
                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);

                StreamEvent oldEvent = state.map.put(generateKey(clonedEvent), clonedEvent);
                if (oldEvent != null) {
                    oldEvent.setTimestamp(currentTime);
                    streamEventChunk.add(oldEvent);
                }
                StreamEvent next = streamEvent.getNext();
                streamEvent.setNext(null);
                streamEventChunk.add(streamEvent);
                streamEvent = next;
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
                streamEvent = ((Operator) compiledCondition).find(matchingEvent, state.map.values(), streamEventCloner);
            }
        } finally {
            stateHolder.returnState(state);
        }
        return streamEvent;
    }

    @Override
    public CompiledCondition compileCondition(Expression expression, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        WindowState state = stateHolder.getState();
        CompiledCondition compiledCondition;
        try {
            compiledCondition = OperatorParser.constructOperator(state.map.values(), expression,
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
        } finally {
            stateHolder.returnState(state);
        }
        return compiledCondition;
    }

    private String generateKey(StreamEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        for (ExpressionExecutor executor : expressionExecutors) {
            stringBuilder.append(executor.execute(event));
        }
        return stringBuilder.toString();
    }

    class WindowState extends State {
        private ConcurrentMap<String, StreamEvent> map = new ConcurrentHashMap<>();

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            return singletonMap("map", this.map);
        }

        @Override
        public void restore(Map<String, Object> state) {
            this.map = (ConcurrentMap<String, StreamEvent>) state.get("map");
        }
    }
}
