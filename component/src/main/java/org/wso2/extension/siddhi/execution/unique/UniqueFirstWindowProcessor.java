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


import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
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
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UniqueFirstWindowProcessor extends WindowProcessor implements FindableProcessor {
    private ConcurrentHashMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();
    private VariableExpressionExecutor variableExpressionExecutors;
    private ConstantExpressionExecutor constantExpressionExecutor;
    private int maxLength = 0;
    private int eventCount = 0;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if(attributeExpressionExecutors.length == 0){
            throw new ExecutionPlanValidationException("UniqueFirst window should have at least one parameter', but found 0");
        }
        else if(attributeExpressionExecutors.length > 2){
            throw new ExecutionPlanValidationException("UniqueFirst window should have at most two parameters, but found "+attributeExpressionExecutors.length);
        }
        else if(attributeExpressionExecutors.length == 1){
            variableExpressionExecutors = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        }
        else{
            variableExpressionExecutors = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            constantExpressionExecutor = (ConstantExpressionExecutor) attributeExpressionExecutors[1];
            if(!constantExpressionExecutor.getReturnType().equals(Attribute.Type.INT)){
                throw new ExecutionPlanValidationException("UniqueFirst window's second parameter must be a constant integer");
            }
            maxLength = (int) constantExpressionExecutor.getValue();
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        synchronized (this) {
            if(constantExpressionExecutor == null) {
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
            else{
                while (streamEventChunk.hasNext()) {
                    StreamEvent streamEvent = streamEventChunk.next();

                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    clonedEvent.setType(StreamEvent.Type.EXPIRED);
                    if(maxLength == eventCount){
                        map.clear();
                        eventCount = 0;
                    }
                    ComplexEvent oldEvent = map.putIfAbsent(generateKey(clonedEvent), clonedEvent);
                    if (oldEvent != null) {
                        streamEventChunk.remove();
                    }
                    eventCount += 1;
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
    public Object[] currentState() {
        return new Object[]{map};
    }

    @Override
    public void restoreState(Object[] state) {
        map = (ConcurrentHashMap<String, StreamEvent>) state[0];
    }

    private String generateKey(StreamEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(event.getAttribute(variableExpressionExecutors.getPosition()));
        return stringBuilder.toString();
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, map.values(), streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        return OperatorParser.constructOperator(map.values(), expression, matchingMetaStateHolder,executionPlanContext,variableExpressionExecutors,eventTableMap);
    }
}
