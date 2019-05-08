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
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
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
 * class representing unique first window processor implementation.
 */

@Extension(
        name = "first",
        namespace = "unique",
        description = "This is a window that holds only the first set of unique events"
                + " according to the unique key parameter."
                + " When a new event arrives with a key that is already in the window,"
                + " that event is not processed by the window.",

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness."
                                + " If there is more than one parameter to check for uniqueness,"
                                + " it can be specified as an array separated by commas.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),
        },
        examples = {
                @Example(
                        syntax = "define stream LoginEvents (timeStamp long, ip string);\n" +
                                "from LoginEvents#window.unique:first(ip)\n" +
                                "insert into UniqueIps ;",

                        description = "This returns the first set of unique items that arrive from the " +
                                "'LoginEvents' stream,"
                                + " and returns them to the 'UniqueIps' stream."
                                + " The unique events are only those with a unique value for the 'ip' attribute."

                )
        }
)

public class UniqueFirstWindowProcessor extends WindowProcessor<UniqueFirstWindowProcessor.ExtensionState>
        implements FindableProcessor {
    private ConcurrentMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();
    private ExpressionExecutor[] uniqueExpressionExecutors;

    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                                ExpressionExecutor[] attributeExpressionExecutors,
                                                ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                                SiddhiQueryContext siddhiQueryContext) {
        uniqueExpressionExecutors = attributeExpressionExecutors;
        return () -> new ExtensionState();
    }

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                     ExtensionState state) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);

                ComplexEvent oldEvent = map.putIfAbsent(generateKey(clonedEvent), clonedEvent);
                if (oldEvent != null) {
                    streamEventChunk.remove();
                }
            }
        }
        nextProcessor.process(streamEventChunk);
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
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    class ExtensionState extends State {

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            return singletonMap("map", UniqueFirstWindowProcessor.this.map);
        }

        @Override
        public void restore(Map<String, Object> map) {
            UniqueFirstWindowProcessor.this.map = (ConcurrentMap<String, StreamEvent>) map.get("map");
        }
    }

    private String generateKey(StreamEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        for (ExpressionExecutor executor : uniqueExpressionExecutors) {
            stringBuilder.append(executor.execute(event));
        }
        return stringBuilder.toString();
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, map.values(),
                    streamEventClonerHolder.getStreamEventCloner());
        } else {
            return null;
        }
    }

    @Override
    public CompiledCondition compileCondition(Expression expression, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> eventTableMap, SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(this.map.values(), expression, matchingMetaInfoHolder,
                variableExpressionExecutors, eventTableMap, siddhiQueryContext);
    }
}
