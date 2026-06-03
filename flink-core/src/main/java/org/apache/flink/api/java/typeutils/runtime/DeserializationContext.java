/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;

/**
 * Thread-local context for tracking the current key and state name during value deserialization.
 * This allows serializers deeper in the call stack to include identifying information in error
 * messages when deserialization fails.
 */
@Internal
public final class DeserializationContext {

    private static final ThreadLocal<Object> currentKey = new ThreadLocal<>();
    private static final ThreadLocal<String> currentStateName = new ThreadLocal<>();

    public static void set(Object key, String stateName) {
        currentKey.set(key);
        currentStateName.set(stateName);
    }

    public static Object getCurrentKey() {
        return currentKey.get();
    }

    public static String getCurrentStateName() {
        return currentStateName.get();
    }

    public static void clear() {
        currentKey.remove();
        currentStateName.remove();
    }

    private DeserializationContext() {}
}
