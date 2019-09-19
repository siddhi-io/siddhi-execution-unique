/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The class representing unique time window processor implementation.
 */

@Extension(
        name = "deduplicate",
        namespace = "unique",
        description = "Removes duplicate events based on the `unique.key` parameter that arrive within " +
                "the `time.interval` gap from one another.",

        parameters = {
                @Parameter(name = "unique.key",
                        description = "Parameter to uniquely identify events.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE, DataType.STRING},
                        dynamic = true),
                @Parameter(name = "time.interval",
                        description = "The sliding time period within which the duplicate events are dropped.",
                        type = {DataType.INT, DataType.LONG})
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"unique.key", "time.interval"})
        },
        examples = {
                @Example(
                        syntax = "define stream TemperatureStream (sensorId string, temperature double)\n\n" +
                                "from TemperatureStream#unique:deduplicate(sensorId, 30 sec)\n" +
                                "select *\n" +
                                "insert into UniqueTemperatureStream;",

                        description = "Query that removes duplicate events of `TemperatureStream` stream " +
                                "based on `sensorId` attribute when they arrive within 30 seconds."
                )
        }
)

public class DeduplicateStreamProcessor extends StreamProcessor<DeduplicateStreamProcessor.DeduplicateState>
        implements SchedulingProcessor {
    private Scheduler scheduler;
    private ExpressionExecutor uniqueExpressionExecutor;
    private long timestampInterval;

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    protected StateFactory<DeduplicateState> init(MetaStreamEvent metaStreamEvent,
                                                  AbstractDefinition inputDefinition,
                                                  ExpressionExecutor[] attributeExpressionExecutors,
                                                  ConfigReader configReader,
                                                  StreamEventClonerHolder streamEventClonerHolder,
                                                  boolean outputExpectsExpiredEvents,
                                                  boolean findToBeExecuted,
                                                  SiddhiQueryContext siddhiQueryContext) {
        timestampInterval = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        uniqueExpressionExecutor = attributeExpressionExecutors[0];
        return DeduplicateStreamProcessor.DeduplicateState::new;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return new ArrayList<>();
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater, DeduplicateState state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            if (state.process(streamEvent, streamEventCloner)) {
                streamEventChunk.remove();
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

    class DeduplicateState extends State {
        private Map<String, StreamEvent> map = new LinkedHashMap<>();

        synchronized boolean process(StreamEvent streamEvent,
                                     StreamEventCloner streamEventCloner) {

            for (Iterator<StreamEvent> iterator = map.values().iterator(); iterator.hasNext(); ) {
                StreamEvent existingEvent = iterator.next();
                if (streamEvent.getTimestamp() - existingEvent.getTimestamp() > timestampInterval) {
                    iterator.remove();
                } else {
                    break;
                }
            }
            if (streamEvent.getType() != ComplexEvent.Type.TIMER) {
                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                String key = uniqueExpressionExecutor.execute(streamEvent).toString();
                StreamEvent existingEvent = map.get(key);
                if (null == existingEvent) {
                    map.put(key, clonedEvent);
                    scheduler.notifyAt(streamEvent.getTimestamp() + timestampInterval);
                    return false;
                }
            }
            return true;

        }

        @Override
        public synchronized boolean canDestroy() {
            return map.isEmpty();
        }

        @Override
        public synchronized Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            Map<String, StreamEvent> newMap = new LinkedHashMap<>();
            newMap.putAll(map);
            state.put("map", newMap);
            return state;
        }

        @Override
        public synchronized void restore(Map<String, Object> state) {
            Map<String, StreamEvent> newMap = new LinkedHashMap<>();
            newMap.putAll((Map) state.get("map"));
            this.map = newMap;
        }
    }
}
