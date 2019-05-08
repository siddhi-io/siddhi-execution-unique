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
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.query.processor.stream.window.WindowProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The class representing unique time window processor implementation.
 */

@Extension(
        name = "time",
        namespace = "unique",
        description = "This is a sliding time window that holds the latest unique events"
                + " that arrived during the previous time window. The unique events are determined"
                + " based on the value for a specified unique key parameter."
                + " The window is updated with the arrival and expiry of each event."
                + " When a new event that arrives within a window time period"
                + " has the same value for the unique key parameter as an existing event in the window,"
                + " the previous event is replaced by the new event.",

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness. ",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "window.time",
                        description = "The sliding time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG})
        },
        examples = {
                @Example(
                        syntax = "define stream CseEventStream (symbol string, price float, volume int)\n" +
                                "from CseEventStream#window.unique:time(symbol, 1 sec)\n" +
                                "select symbol, price, volume\n" +
                                "insert expired events into OutputStream ;",

                        description = "In this query, the window holds the latest unique events"
                                + " that arrived within the last second from the 'CseEventStream',"
                                + " and returns the expired events to the 'OutputStream' stream."
                                + " During any given second, each event in the window should have"
                                + " a unique value for the 'symbol' attribute. If a new event that arrives"
                                + " within the same second has the same value for the symbol attribute"
                                + " as an existing event in the window, the existing event expires."
                )
        }
)

public class UniqueTimeWindowProcessor extends WindowProcessor<UniqueTimeWindowProcessor.ExtensionState>
        implements SchedulingProcessor, FindableProcessor {

    private long timeInMilliSeconds;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;
    private Scheduler scheduler;
    private volatile long lastTimestamp = Long.MIN_VALUE;
    private ExpressionExecutor uniqueKeyExpressionExecutor;


    @Override
    public synchronized Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public synchronized void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                                ExpressionExecutor[] attributeExpressionExecutors,
                                                ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                                SiddhiQueryContext siddhiQueryContext) {
        this.expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        if (attributeExpressionExecutors.length == 2) {
            uniqueKeyExpressionExecutor = attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();

                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                } else {
                    throw new SiddhiAppValidationException(
                            "UniqueTime window's parameter time should be either" + " int or long, but found "
                                    + attributeExpressionExecutors[0].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException(
                        "UniqueTime window should have constant for time parameter but " + "found a dynamic attribute "
                                + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException("UniqueTime window should only have two parameters "
                    + "(<string|int|bool|long|double|float> unique attribute, <int|long|time> windowTime), but found "
                    + attributeExpressionExecutors.length + " input attributes");
        }
        return () -> new ExtensionState();
    }

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                     ExtensionState state) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
                StreamEvent oldEvent = null;
                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    clonedEvent.setType(StreamEvent.Type.EXPIRED);
                    StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(streamEvent);
                    eventClonedForMap.setType(StreamEvent.Type.EXPIRED);
                    oldEvent = state.map.put(generateKey(eventClonedForMap), eventClonedForMap);
                    this.expiredEventChunk.add(clonedEvent);
                    if (lastTimestamp < clonedEvent.getTimestamp()) {
                        if (scheduler != null) {
                            scheduler.notifyAt(clonedEvent.getTimestamp() + timeInMilliSeconds);
                            lastTimestamp = clonedEvent.getTimestamp();
                        }
                    }
                }
                expiredEventChunk.reset();
                while (expiredEventChunk.hasNext()) {
                    StreamEvent expiredEvent = expiredEventChunk.next();
                    long timeDiff = expiredEvent.getTimestamp() - currentTime + timeInMilliSeconds;
                    if (timeDiff <= 0 || oldEvent != null) {
                        if (oldEvent != null) {
                            if (expiredEvent.equals(oldEvent)) {
                                this.expiredEventChunk.remove();
                                streamEventChunk.insertBeforeCurrent(oldEvent);
                                oldEvent.setTimestamp(currentTime);
                                oldEvent = null;
                            }
                        } else {
                            expiredEventChunk.remove();
                            expiredEvent.setTimestamp(currentTime);
                            streamEventChunk.insertBeforeCurrent(expiredEvent);
                            expiredEvent.setTimestamp(currentTime);
                            expiredEventChunk.reset();
                        }
                    } else {
                        break;
                    }
                }
                expiredEventChunk.reset();
                if (streamEvent.getType() != StreamEvent.Type.CURRENT) {
                    streamEventChunk.remove();
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, expiredEventChunk,
                    streamEventClonerHolder.getStreamEventCloner());
        } else {
            return null;
        }
    }

    @Override
    public CompiledCondition compileCondition(Expression expression, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(expiredEventChunk, expression, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
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

        private ConcurrentMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> map = new HashMap<>();
            map.put("expiredEventchunck", expiredEventChunk.getFirst());
            map.put("map", this.map);
            return map;
        }

        @Override
        public void restore(Map<String, Object> state) {
            expiredEventChunk.clear();
            expiredEventChunk.add((StreamEvent) map.get("expiredEventchunck"));
            this.map = (ConcurrentMap<String, StreamEvent>) state.get("map");
        }
    }

    /**
     * Used to generate key in map to get the old event for current event. It will map key which we give as unique
     * attribute with the event
     *
     * @param event the stream event that need to be processed
     */
    private String generateKey(StreamEvent event) {
        return uniqueKeyExpressionExecutor.execute(event).toString();
    }

}
