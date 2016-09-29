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
package org.wso2.extension.siddhi.window.uniquelengthbatch;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * UniqueLengthBatch Window
 * Sample Query:
 * from inputStream#window.unique:lengthBatch(attribute1,3,true)
 * select attribute1, attribute2
 * insert into outputStream;
 *
 * Description:
 * In the example query given, 3 is the length of the window and attribute1 is the unique attribute.
 * According to the given attribute it will give length number of unique events .
 * attribute true is to keep first unique default value is last unique.
 *
 * @since 1.0.0
 */
public class UniqueLengthBatchWindowProcessor extends WindowProcessor implements FindableProcessor {

    private int windowLength;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ComplexEventChunk<StreamEvent> eventsToBeExpired = null;
    private ExecutionPlanContext executionPlanContext;
    private StreamEvent resetEvent = null;
    private VariableExpressionExecutor uniqueKey;
    private Map<Object, StreamEvent> uniqueEventMap = new HashMap<>();
    private boolean isFirstUniqueEnabled = false;

    /**
     * The init method of the WindowProcessor, this method will be called before other methods.
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        this.eventsToBeExpired = new ComplexEventChunk<>(false);
        if (attributeExpressionExecutors.length == 2) {
            this.uniqueKey = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    this.windowLength = (Integer)
                            (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
                } else {
                    throw new ExecutionPlanValidationException("Length should be an INT");
                }
            } else {
                throw new ExecutionPlanValidationException("Unique Length Batch window should have constant " +
                        "for length parameter but found a dynamic attribute "
                        + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else if (attributeExpressionExecutors.length == 3) {
            this.uniqueKey = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    this.windowLength = (Integer)
                            (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
                } else {
                    throw new ExecutionPlanValidationException("Length should be an INT");
                }
            } else {
                throw new ExecutionPlanValidationException("Unique Length Batch window should have constant " +
                        "for length parameter but found a dynamic attribute "
                        + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
                    this.isFirstUniqueEnabled = (boolean) (((ConstantExpressionExecutor)
                            attributeExpressionExecutors[2]).getValue());
                } else {
                    throw new ExecutionPlanValidationException("Third Value should be boolean value");
                }
            }
        } else {
            throw new ExecutionPlanValidationException("Unique Length batch window should only have Two/Three parameter, " +
                    "but found " + attributeExpressionExecutors.length + " input attributes");
        }
    }

    /**
     * The main processing method that will be called upon event arrival.
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                if (!isFirstUniqueEnabled) {
                    uniqueEventMap.put(clonedStreamEvent
                            .getAttribute(uniqueKey.getPosition()), clonedStreamEvent);
                } else {
                    if (!uniqueEventMap.containsKey(clonedStreamEvent
                            .getAttribute(uniqueKey.getPosition()))) {
                        uniqueEventMap.put(clonedStreamEvent
                                .getAttribute(uniqueKey.getPosition()), clonedStreamEvent);
                    }
                }
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

    /**
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {
        //Do nothing
    }

    /**
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
        //Do nothing
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time.
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Object[] currentState() {
        if (eventsToBeExpired != null) {
            return new Object[]{currentEventChunk.getFirst(), eventsToBeExpired.getFirst(), count, resetEvent};
        } else {
            return new Object[]{currentEventChunk.getFirst(), count, resetEvent};
        }
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on
     *              the same order provided by currentState().
     */
    @Override
    public void restoreState(Object[] state) {
        if (state.length > 3) {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            eventsToBeExpired.clear();
            eventsToBeExpired.add((StreamEvent) state[1]);
            count = (Integer) state[2];
            resetEvent = (StreamEvent) state[3];
        } else {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            count = (Integer) state[1];
            resetEvent = (StreamEvent) state[2];
        }
    }

    /**
     * To find events from the processor event pool, that the matches the matchingEvent based on finder logic.
     *
     * @param matchingEvent the event to be matched with the events at the processor
     * @param finder        the execution element responsible for finding the corresponding events
     *                      that matches the matchingEvent based on pool of events at Processor
     * @return the matched events
     */
    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, uniqueEventMap.values(), streamEventCloner);
    }

    /**
     * To construct a finder having the capability of finding events at the processor that corresponds
     * to the incoming matchingEvent and the given matching expression logic.
     *
     * @param expression                  the matching expression
     * @param matchingMetaStateHolder     the meta structure of the incoming matchingEvent
     * @param executionPlanContext        current execution plan context
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param eventTableMap               uniqueEventMap of event tables
     * @return finder having the capability of finding events at the processor against the expression
     * and incoming matchingEvent
     */
    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        if (eventsToBeExpired == null) {
            eventsToBeExpired = new ComplexEventChunk<StreamEvent>(false);
        }
        return OperatorParser.constructOperator(uniqueEventMap.values(), expression,
                matchingMetaStateHolder, executionPlanContext, variableExpressionExecutors, eventTableMap);
    }
}