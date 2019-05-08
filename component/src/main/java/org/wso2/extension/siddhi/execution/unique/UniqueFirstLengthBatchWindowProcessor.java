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

package org.wso2.extension.siddhi.execution.unique;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.executor.ExpressionExecutor;

import java.util.Map;

/**
 *  Class representing unique first length batch window processor implementation.
 */

@Extension(
        name = "firstLengthBatch",
        namespace = "unique",
        description = "This is a batch (tumbling) window that holds a specific number of unique events"
                + " (depending on which events arrive first). The unique events are selected based"
                + " on a specific parameter that is considered as the unique key."
                + " When a new event arrives with a value for the unique key parameter"
                + " that matches the same of an existing event in the window,"
                + " that event is not processed by the window." ,

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "window.length",
                        description = "The number of events the window should tumble.",
                        type = {DataType.INT}),
        },
        examples = {
                @Example(
                        syntax = "define window CseEventWindow (symbol string, price float, volume int) " +
                                "from CseEventStream#window.unique:firstLengthBatch(symbol, 10)\n" +
                                "select symbol, price, volume\n" +
                                "insert all events into OutputStream ;",
                        description = "The window in this configuration holds the first unique events"
                                + " from the 'CseEventStream' stream every second, and"
                                + " outputs them all into the the 'OutputStream' stream."
                                + " All the events in a window during a given second should"
                                + " have a unique value for the 'symbol' attribute."
                )
        }
)

public class UniqueFirstLengthBatchWindowProcessor extends UniqueLengthBatchWindowProcessor {
    @Override protected void addUniqueEvent(Map<Object, StreamEvent> uniqueEventMap,
                                            ExpressionExecutor uniqueKeyExpressionExecutor,
                                            StreamEvent clonedStreamEvent) {
        String uniqueKey = uniqueKeyExpressionExecutor.execute(clonedStreamEvent).toString();
        if (!uniqueEventMap.containsKey(uniqueKey)) {
            uniqueEventMap.put(uniqueKey, clonedStreamEvent);
        }
    }
}
