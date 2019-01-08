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
package org.wso2.extension.siddhi.execution.unique;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

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
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "window.length",
                        description = "The number of events that should be "
                                + "included in a sliding length window.",
                        type = {DataType.INT})
        },
        examples = @Example(
                syntax = "define stream CseEventStream (symbol string, price float, volume int)\n" +
                        "from CseEventStream#window.unique:length(symbol,10)\n" +
                        "select symbol, price, volume\n" +
                        "insert all events into OutputStream ;",

                description = "In this configuration, the window holds the latest 10 unique events."
                        + " The latest events are selected based on the symbol attribute. "
                        + "If the 'CseEventStream' receives an event for which the value for the symbol attribute "
                        + "is the same as that of an existing event in the window,"
                        + " the existing event is replaced by the new event. "
                        + "All the events are returned to the 'OutputStream' event stream "
                        + "once an event expires or is added to the window."
        )
)

public class UniqueLengthWindowProcessor extends WindowProcessor implements FindableProcessor {
    private ConcurrentHashMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();
    private ExpressionExecutor uniqueKeyExpressionExecutor;
    private int length;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        boolean b, SiddhiAppContext siddhiAppContext) {
        expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        if (attributeExpressionExecutors.length == 2) {
            uniqueKeyExpressionExecutor = attributeExpressionExecutors[0];
            length = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        } else {
            throw new SiddhiAppValidationException("Unique Length window should only have two parameters "
                    + "(<string|int|bool|long|double|float> attribute, <int> windowLength), but found "
                    + attributeExpressionExecutors.length + " input attributes");
        }

    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                streamEvent.setNext(null);
                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);
                StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(clonedEvent);
                StreamEvent oldEvent = map.put(generateKey(eventClonedForMap), eventClonedForMap);
                if (oldEvent == null) {
                    count++;
                }
                if ((count <= length) && (oldEvent == null)) {
                    this.expiredEventChunk.add(clonedEvent);
                } else {
                    if (oldEvent != null) {
                        while (expiredEventChunk.hasNext()) {
                            StreamEvent firstEventExpired = expiredEventChunk.next();
                            if (firstEventExpired.equals(oldEvent)) {
                                this.expiredEventChunk.remove();
                            }
                        }
                        this.expiredEventChunk.add(clonedEvent);
                        streamEventChunk.insertBeforeCurrent(oldEvent);
                        oldEvent.setTimestamp(currentTime);
                    } else {
                        StreamEvent firstEvent = this.expiredEventChunk.poll();
                        if (firstEvent != null) {
                            firstEvent.setTimestamp(currentTime);
                            streamEventChunk.insertBeforeCurrent(firstEvent);
                            this.expiredEventChunk.add(clonedEvent);
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
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public synchronized Map<String, Object> currentState() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("expiredEventChunk", expiredEventChunk.getFirst());
        map.put("count", count);
        map.put("map", this.map);
        return map;
    }

    @Override
    public synchronized void restoreState(Map<String, Object> map) {
        expiredEventChunk.clear();
        expiredEventChunk.add((StreamEvent) map.get("expiredEventChunk"));
        count = (Integer) map.get("count");
        this.map = (ConcurrentHashMap) map.get("map");
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, expiredEventChunk, streamEventCloner);
        } else {
            return null;
        }
    }

    private String generateKey(StreamEvent event) {
        return uniqueKeyExpressionExecutor.execute(event).toString();
    }

    @Override
    public CompiledCondition compileCondition(Expression expression,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              SiddhiAppContext siddhiAppContext,
                                              List<VariableExpressionExecutor> list, Map<String, Table> map,
                                              String queryName) {
        return OperatorParser.constructOperator(expiredEventChunk, expression, matchingMetaInfoHolder,
                siddhiAppContext, list, map, queryName);
    }
}
