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
        description = "Window that retains the latest events based on a given unique keys. " +
                "When a new event arrives with the same key it replaces the one that exist in the window." +
                "<b>This function is not recommended to be used when the maximum number of unique attributes " +
                "are undefined, as there is a risk of system going out to memory</b>.",

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute used to checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE, DataType.STRING},
                        dynamic = true),
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"unique.key"}),
                @ParameterOverload(parameterNames = {"unique.key", "..."}),
        },
        examples = {
                @Example(
                        syntax = "define stream LoginEvents (timestamp long, ip string);\n\n" +
                                "from LoginEvents#window.unique:ever(ip)\n" +
                                "select count(ip) as ipCount\n" +
                                "insert events into UniqueIps;",

                        description = "Query collects all unique events based on the `ip` attribute by " +
                                "retaining the latest unique events from the `LoginEvents` stream. " +
                                "Then the query counts the unique `ip`s arrived so far and outputs the `ipCount`" +
                                " via the `UniqueIps` stream."
                ),
                @Example(
                        syntax = "define stream DriverChangeStream (trainID string, driver string);\n\n" +
                                "from DriverChangeStream#window.unique:ever(trainID)\n" +
                                "select trainID, driver\n" +
                                "insert expired events into PreviousDriverChangeStream;",

                        description = "Query collects all unique events based on the `trainID` attribute by " +
                                "retaining the latest unique events from the `DriverChangeStream` stream. " +
                                "The query outputs the previous unique event stored in the window as the " +
                                "expired events are emitted via `PreviousDriverChangeStream` stream."
                ),
                @Example(
                        syntax = "define stream StockStream (symbol string, price float);\n" +
                                "define stream PriceRequestStream(symbol string);\n" +
                                "\n" +
                                "from StockStream#window.unique:ever(symbol) as s join PriceRequestStream as p\n" +
                                "on s.symbol == p.symbol \n" +
                                "select s.symbol as symbol, s.price as price \n" +
                                "insert events into PriceResponseStream;",

                        description = "Query stores the last unique event for each `symbol` attribute of " +
                                "`StockStream` stream, and joins them with events arriving on the " +
                                "`PriceRequestStream` for equal `symbol` attributes to fetch the latest " +
                                "`price` for each requested `symbol` and output via `PriceResponseStream` stream."
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
                streamEvent = ((Operator) compiledCondition).find(matchingEvent, state.map.values(),
                        streamEventCloner);
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
