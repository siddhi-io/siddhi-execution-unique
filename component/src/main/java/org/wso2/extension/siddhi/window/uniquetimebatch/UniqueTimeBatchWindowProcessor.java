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
package org.wso2.extension.siddhi.window.uniquetimebatch;

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
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * UniqueTimeBatch window
 * Sample Query:
 * from inputStream#window.unique:timeBatch(attribute,3 sec)
 * select attribute1, attribute2
 * insert into outputStream;
 *
 * Description:
 * In the example query given, 3 is the duration of the window and attribute is the unique attribute.
 * According to the given attribute it will give unique events within each given time batch.
 * attribute true is to keep first unique default value is last unique.
 *
 * @since 1.0.0
 */
public class UniqueTimeBatchWindowProcessor extends WindowProcessor implements SchedulingProcessor,
        FindableProcessor {

    private long timeInMilliSeconds;
    private long nextEmitTime = -1;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<>(false);
    private ComplexEventChunk<StreamEvent> eventsToBeExpired = null;
    private Map<Object, StreamEvent> uniqueEventMap = new HashMap<>();
    private StreamEvent resetEvent = null;
    private Scheduler scheduler;
    private ExecutionPlanContext executionPlanContext;
    private boolean isStartTimeEnabled = false;
    private long startTime = 0;
    private VariableExpressionExecutor uniqueKey;
    private boolean isFirstUniqueEnabled = false;

    /**
     * The setScheduler method of the TimeWindowProcessor, As scheduler is private variable,
     * to access publicly we use this setter method.
     *
     * @param scheduler the value of scheduler.
     */
    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * The getScheduler method of the TimeWindowProcessor, As scheduler is private variable,
     * to access publicly we use this getter method.
     */
    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * The init method of the WindowProcessor, this method will be called before other methods.
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors,
                        ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        this.eventsToBeExpired = new ComplexEventChunk<>(false);
        if (attributeExpressionExecutors.length == 2) {
            if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
                this.uniqueKey = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            } else {
                throw new ExecutionPlanValidationException("Unique Length Batch window should have variable " +
                        "for Unique Key parameter but found an attribute " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer)
                            ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long)
                            ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("Unique Time Batch window's parameter time should be either" +
                            " int or long, but found " + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException("Unique Time Batch window should have constant " +
                        "for time parameter but found a dynamic attribute "
                        + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else if (attributeExpressionExecutors.length == 3) {
            if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
                this.uniqueKey = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            } else {
                throw new ExecutionPlanValidationException("Unique Length Batch window should have variable " +
                        "for Unique Key parameter but found an attribute " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer)
                            ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long)
                            ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("UniqueTimeBatch window's parameter time should be either" +
                            " int or long, but found " + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException("Unique Time Batch window should have constant " +
                        "for time parameter but found a dynamic attribute "
                        + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            // isStartTimeEnabled used to set start time
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
                    this.isFirstUniqueEnabled = (boolean) (((ConstantExpressionExecutor)
                            attributeExpressionExecutors[2]).getValue());

                } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    isStartTimeEnabled = true;
                    startTime = Integer.parseInt(String
                            .valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
                } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                    isStartTimeEnabled = true;
                    startTime = Long.parseLong(String
                            .valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
                } else {
                    throw new ExecutionPlanValidationException("Expected either boolean, int or long type for UniqueTimeBatch window's third parameter, but found "
                            + attributeExpressionExecutors[2].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException("Unique Time Batch window should have constant " +
                        "for time parameter but found a dynamic attribute "
                        + attributeExpressionExecutors[2].getReturnType());
            }
        } else if (attributeExpressionExecutors.length == 4) {
            if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
                this.uniqueKey = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            } else {
                throw new ExecutionPlanValidationException("Unique Length Batch window should have variable " +
                        "for Unique Key parameter but found an attribute " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer)
                            ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long)
                            ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("UniqueTimeBatch window's parameter time should be either" +
                            " int or long, but found " + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException("Unique Time Batch window should have constant " +
                        "for time parameter but found a dynamic attribute "
                        + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            // isStartTimeEnabled used to set start time
            isStartTimeEnabled = true;
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    startTime = Integer.parseInt(String
                            .valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
                } else {
                    startTime = Long.parseLong(String
                            .valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
                }
            } else {
                throw new ExecutionPlanValidationException("Unique Time Batch window's parameter startTime should be either" +
                        " int or long, but found " + attributeExpressionExecutors[2].getReturnType());
            }
            if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                this.isFirstUniqueEnabled = (boolean) (((ConstantExpressionExecutor)
                        attributeExpressionExecutors[3]).getValue());
            } else {
                throw new ExecutionPlanValidationException("Unique Time Batch window's parameter isFirstUniqueEnabled should be" +
                        " bool, but found " + attributeExpressionExecutors[3].getReturnType());
            }
        } else {
            throw new ExecutionPlanValidationException("Unique Time Batch window should only have two or Three or four parameters. " +
                    "but found " + attributeExpressionExecutors.length + " input attributes");
        }
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        synchronized (this) {
            if (nextEmitTime == -1) {
                long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
                if (isStartTimeEnabled) {
                    nextEmitTime = getNextEmitTime(currentTime);
                } else {
                    nextEmitTime = executionPlanContext.getTimestampGenerator().currentTime()
                            + timeInMilliSeconds;
                }
                scheduler.notifyAt(nextEmitTime);
            }
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            boolean sendEvents;
            if (currentTime >= nextEmitTime) {
                nextEmitTime += timeInMilliSeconds;
                scheduler.notifyAt(nextEmitTime);
                sendEvents = true;
            } else {
                sendEvents = false;
            }
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
            }
            streamEventChunk.clear();
            if (sendEvents) {
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
        }
        if (streamEventChunk.getFirst() != null) {
            streamEventChunk.setBatch(true);
            nextProcessor.process(streamEventChunk);
            streamEventChunk.setBatch(false);
        }
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

    /**
     * This will be called after initializing the system and before starting to process the events.
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
            return new Object[]{currentEventChunk.getFirst(), eventsToBeExpired.getFirst(), resetEvent};
        } else {
            return new Object[]{currentEventChunk.getFirst(), resetEvent};
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
        if (state.length > 2) {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            eventsToBeExpired.clear();
            eventsToBeExpired.add((StreamEvent) state[1]);
            resetEvent = (StreamEvent) state[2];
        } else {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            resetEvent = (StreamEvent) state[1];
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
        return finder.find(matchingEvent, eventsToBeExpired, streamEventCloner);
    }

    /**
     * To construct a finder having the capability of finding events at the processor that corresponds
     * to the incoming matchingEvent and the given matching expression logic.
     *
     * @param expression                  the matching expression
     * @param executionPlanContext        current execution plan context
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param eventTableMap               map of event tables
     * @return finder having the capability of finding events at the processor against the expression
     * and incoming matchingEvent
     */
    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
                                  ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors,
                                  Map<String, EventTable> eventTableMap) {
        if (eventsToBeExpired == null) {
            eventsToBeExpired = new ComplexEventChunk<StreamEvent>(false);
        }
        return OperatorParser.constructOperator(eventsToBeExpired, expression, matchingMetaStateHolder,
                executionPlanContext, variableExpressionExecutors, eventTableMap);
    }
}