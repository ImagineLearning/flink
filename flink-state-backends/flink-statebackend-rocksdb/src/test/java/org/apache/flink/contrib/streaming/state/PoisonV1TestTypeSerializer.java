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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DeserializationContext;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.runtime.testutils.statemigration.TestType.TestTypeSerializerBase;

import java.io.IOException;

/**
 * A V1-schema serializer for {@link TestType} that WRITES valid bytes but FAILS to read, simulating
 * the QA "toxic state" scenario.
 *
 * <p>It declares itself {@code compatibleAfterMigration} against itself, so registering state with
 * a fresh instance on restore forces {@code RocksDBKeyedStateBackend.migrateStateValues} to run.
 * That migration loop reads each value with the prior serializer ({@code migrateSerializedValue} ->
 * {@code priorSerializer.deserialize}), which throws here.
 *
 * <p>Before throwing, it captures whatever {@link DeserializationContext} holds. PR #22 (JET-2518)
 * populates that context with the keyed-state key + state name during migration; without the PR it
 * is empty. Tests assert on the captured values, giving a clean base-vs-PR diff that does not
 * depend on PojoSerializer or the CONTINUE_ON_POJO_DESERIALIZATION_FAILURE env flag.
 */
public class PoisonV1TestTypeSerializer extends TestTypeSerializerBase {

    private static final long serialVersionUID = 1L;

    /**
     * Captured from {@link DeserializationContext} at the moment {@link #deserialize} is called
     * during migration.
     */
    public static volatile Object capturedKey;

    public static volatile String capturedStateName;

    public static volatile boolean deserializeCalled;

    public static void reset() {
        capturedKey = null;
        capturedStateName = null;
        deserializeCalled = false;
    }

    @Override
    public void serialize(TestType record, DataOutputView target) throws IOException {
        // Valid V1 bytes so the entry is written to RocksDB cleanly.
        target.writeUTF(record.getKey());
        target.writeInt(record.getValue());
    }

    @Override
    public TestType deserialize(DataInputView source) throws IOException {
        deserializeCalled = true;
        // What PR #22 threaded in via the migration loop:
        capturedKey = DeserializationContext.getCurrentKey();
        capturedStateName = DeserializationContext.getCurrentStateName();
        // Simulate the corrupt/toxic value that cannot be deserialized.
        throw new IOException("Simulated toxic state: cannot deserialize value");
    }

    @Override
    public TypeSerializerSnapshot<TestType> snapshotConfiguration() {
        return new PoisonV1TestTypeSerializerSnapshot();
    }

    /** Snapshot that forces migration ({@code compatibleAfterMigration}) against itself. */
    public static class PoisonV1TestTypeSerializerSnapshot
            implements TypeSerializerSnapshot<TestType> {

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {}

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader cl)
                throws IOException {}

        @Override
        public TypeSerializer<TestType> restoreSerializer() {
            // The migration loop reads old bytes with THIS serializer -> deserialize() throws.
            return new PoisonV1TestTypeSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<TestType> resolveSchemaCompatibility(
                TypeSerializerSnapshot<TestType> oldSerializerSnapshot) {
            if (oldSerializerSnapshot instanceof PoisonV1TestTypeSerializerSnapshot) {
                // This is what makes the backend choose migrateStateValues() over a plain restore.
                return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
            }
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
