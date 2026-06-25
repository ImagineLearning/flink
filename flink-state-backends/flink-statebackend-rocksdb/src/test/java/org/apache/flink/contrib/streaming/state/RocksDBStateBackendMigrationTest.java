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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendMigrationTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the partitioned state part of {@link RocksDBStateBackend}. */
@ExtendWith(ParameterizedTestExtension.class)
public class RocksDBStateBackendMigrationTest
        extends StateBackendMigrationTestBase<RocksDBStateBackend> {

    @Parameters(name = "Incremental checkpointing: {0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameter public boolean enableIncrementalCheckpointing;

    // Store it because we need it for the cleanup test.
    private String dbPath;

    @Override
    protected RocksDBStateBackend getStateBackend() throws IOException {
        dbPath = TempDirUtils.newFolder(tempFolder).getAbsolutePath();
        String checkpointPath = TempDirUtils.newFolder(tempFolder).toURI().toString();
        RocksDBStateBackend backend =
                new RocksDBStateBackend(
                        new FsStateBackend(checkpointPath), enableIncrementalCheckpointing);

        Configuration configuration = new Configuration();
        configuration.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
        backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
        backend.setDbStoragePath(dbPath);
        return backend;
    }

    /**
     * Reproduces the QA "toxic state" migration failure and verifies PR #22 (JET-2518): during
     * RocksDB state migration, a value that fails to deserialize is now attributable to a concrete
     * keyed-state key and state name via {@code DeserializationContext}.
     *
     * <p>Run on base (release-1.20.3.il): capturedKey == null.
     *
     * <p>Run on PR #22 (feature/JET-2518): capturedKey == the real key, capturedStateName ==
     * "state".
     *
     * <p>Self-contained: it builds, snapshots and restores the keyed backend with public APIs only,
     * so it does not depend on any (private) helpers of the base class.
     */
    @TestTemplate
    void testMigrationFailureSurfacesStateAndKey() throws Exception {
        final String stateName = "state"; // matches AbstractRadStatefulFunction.STATE_NAME
        final String toxicKey =
                "b5b5cf29-d27a-4577-8057-fd51f529d25f/868d144c-2c86-4ed3-b4ef-520d86eb0986/8311179812";

        PoisonV1TestTypeSerializer.reset();

        MockEnvironment env = MockEnvironment.builder().build();
        try {
            CheckpointStreamFactory streamFactory = createStreamFactory(env);
            SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

            // --- phase 1: write one entry with the poison V1 serializer (writes valid bytes) ---
            CheckpointableKeyedStateBackend<String> backend = createKeyedBackend(env);

            KeyedStateHandle snapshot;
            try {
                ValueState<TestType> state =
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                new ValueStateDescriptor<>(
                                        stateName, new PoisonV1TestTypeSerializer()));

                backend.setCurrentKey(toxicKey);
                state.update(new TestType(toxicKey, 12345));

                snapshot =
                        runSnapshot(
                                backend.snapshot(
                                        1L,
                                        2L,
                                        streamFactory,
                                        CheckpointOptions.forCheckpointWithDefaultLocation()),
                                sharedStateRegistry);
            } finally {
                backend.dispose();
            }

            // --- phase 2: restore; registering the poison serializer forces migration, throws ---
            final CheckpointableKeyedStateBackend<String> restoredBackend =
                    restoreKeyedBackend(env, snapshot);
            try {
                // getPartitionedState -> updateRestoredStateMetaInfo -> migrateStateValues: the
                // migration loop reads each value with the prior (poison) serializer, which throws,
                // so this call propagates the failure.
                assertThatThrownBy(
                                () ->
                                        restoredBackend.getPartitionedState(
                                                VoidNamespace.INSTANCE,
                                                VoidNamespaceSerializer.INSTANCE,
                                                new ValueStateDescriptor<>(
                                                        stateName,
                                                        new PoisonV1TestTypeSerializer())))
                        .isNotNull();
            } finally {
                restoredBackend.dispose();
            }

            // --- assertions: prove migration ran AND PR #22 populated the context ---
            assertThat(PoisonV1TestTypeSerializer.deserializeCalled)
                    .as("migration must have invoked the prior serializer's deserialize()")
                    .isTrue();

            // THE PR #22 CHECK. On base these are null; on the PR they carry the real identity.
            assertThat(PoisonV1TestTypeSerializer.capturedStateName).isEqualTo(stateName);
            assertThat(PoisonV1TestTypeSerializer.capturedKey).hasToString(toxicKey);
        } finally {
            IOUtils.closeQuietly(env);
        }
    }

    // -------------------------------------------------------------------------------
    //  Minimal inlined backend/snapshot helpers (public APIs only)
    // -------------------------------------------------------------------------------

    private CheckpointStreamFactory createStreamFactory(MockEnvironment env) throws Exception {
        CheckpointStorageAccess access =
                ((CheckpointStorage) getStateBackend()).createCheckpointStorage(new JobID());
        access.initializeBaseLocationsForCheckpoint();
        env.setCheckpointStorageAccess(access);
        return access.initializeLocationForCheckpoint(1L);
    }

    private CheckpointableKeyedStateBackend<String> createKeyedBackend(MockEnvironment env)
            throws Exception {
        return buildBackend(env, Collections.emptyList());
    }

    private CheckpointableKeyedStateBackend<String> restoreKeyedBackend(
            MockEnvironment env, KeyedStateHandle stateHandle) throws Exception {
        return buildBackend(env, Collections.singletonList(stateHandle));
    }

    private CheckpointableKeyedStateBackend<String> buildBackend(
            MockEnvironment env, Collection<KeyedStateHandle> stateHandles) throws Exception {
        StateBackend stateBackend = getStateBackend();
        return stateBackend.createKeyedStateBackend(
                new KeyedStateBackendParametersImpl<>(
                        env,
                        new JobID(),
                        "test_op",
                        StringSerializer.INSTANCE,
                        10,
                        new KeyGroupRange(0, 9),
                        env.getTaskKvStateRegistry(),
                        org.apache.flink.runtime.state.ttl.TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        stateHandles,
                        new CloseableRegistry()));
    }

    private KeyedStateHandle runSnapshot(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture,
            SharedStateRegistry sharedStateRegistry)
            throws Exception {
        if (!snapshotRunnableFuture.isDone()) {
            snapshotRunnableFuture.run();
        }
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshotRunnableFuture.get();
        KeyedStateHandle jobManagerOwnedSnapshot = snapshotResult.getJobManagerOwnedSnapshot();
        if (jobManagerOwnedSnapshot != null) {
            jobManagerOwnedSnapshot.registerSharedStates(sharedStateRegistry, 0L);
        }
        return jobManagerOwnedSnapshot;
    }
}
