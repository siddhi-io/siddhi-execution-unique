/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.execution.unique;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.executor.ExpressionExecutor;

import java.util.Map;

/**
 * Class representing unique first time batch Window processor implementation.
 */

@Extension(
        name = "firstTimeBatch",
        namespace = "unique",
        description = "A batch-time or tumbling window that holds the unique events"
                + " according to the unique key parameters that have arrived within the time period of that window"
                + " and gets updated for each such time window."
                + " When a new event arrives with a key which is already in the window,"
                + " that event is not processed by the window.",
        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE, DataType.STRING},
                        dynamic = true),
                @Parameter(name = "window.time",
                        description = "The sliding time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG},
                        dynamic = true),
                @Parameter(name = "start.time",
                        description = "This specifies an offset in milliseconds in order to start the " +
                                "window at a time different to the standard time.",
                        defaultValue = "Timestamp of the first event.",
                        type = {DataType.INT, DataType.LONG},
                        dynamic = true,
                        optional = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"unique.key", "window.time"}),
                @ParameterOverload(parameterNames = {"unique.key", "window.time", "start.time"}),
        },
        examples = {
                @Example(
                        syntax = "define stream CseEventStream (symbol string, price float, volume int)\n\n" +
                                "from CseEventStream#window.unique:firstTimeBatch(symbol,1 sec)\n " +
                                "select symbol, price, volume\n" +
                                "insert all events into OutputStream ;",

                        description = "This holds the first unique events that arrive from the 'cseEventStream' " +
                                "input stream during each second, based on the symbol," +
                                "as a batch, and returns all the events to the 'OutputStream'. "
                )
        }
)

public class UniqueFirstTimeBatchWindowProcessor extends UniqueTimeBatchWindowProcessor {
    @Override
    protected void addUniqueEvent(Map<Object, StreamEvent> uniqueEventMap,
                                  ExpressionExecutor uniqueKeyExpressionExecutor,
                                  StreamEvent clonedStreamEvent) {
        String uniqueKey = uniqueKeyExpressionExecutor.execute(clonedStreamEvent).toString();
        if (!uniqueEventMap.containsKey(uniqueKey)) {
            uniqueEventMap.put(uniqueKey, clonedStreamEvent);
        }
    }
}
