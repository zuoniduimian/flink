/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/** Test of different implementation of {@link RecoveredChannelStateHandler}. */
@RunWith(Parameterized.class)
public class RecoveredChannelStateHandlerTest<Info, Context> {
    RecoveredChannelStateHandlerTester<Info, Context> stateHandlerTester;

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[][] {
            {new ResultSubpartitionRecoveredChannelStateHandlerTester()},
            {new InputChannelRecoveredStateHandlerTester()},
        };
    }

    public RecoveredChannelStateHandlerTest(
            RecoveredChannelStateHandlerTester<Info, Context> stateHandlerTester) {
        this.stateHandlerTester = stateHandlerTester;
    }

    @Test
    public void testRecycleBufferBeforeRecoverWasCalled() throws Exception {
        // given: Initialized state handler.
        stateHandlerTester.init();

        Info channelInfo = stateHandlerTester.getInfo();

        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Context> buffer =
                stateHandlerTester.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        buffer.buffer.recycle();

        // then: Buffer should be recycled the same times as it was retained.
        stateHandlerTester.assertState();
    }

    @Test
    public void testRecycleBufferAfterRecoverWasCalled() throws Exception {
        // given: Initialized state handler.
        stateHandlerTester.init();

        Info channelInfo = stateHandlerTester.getInfo();

        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Context> buffer =
                stateHandlerTester.getBuffer(channelInfo);

        // and: Pass the buffer to recovery.
        stateHandlerTester.recover(channelInfo, 0, buffer.context);

        // and: Recycle buffer outside.
        buffer.buffer.recycle();

        // then: Buffer should be recycled the same times as it was retained.
        stateHandlerTester.assertState();
    }

    static class ResultSubpartitionRecoveredChannelStateHandlerTester
            extends RecoveredChannelStateHandlerTester<ResultSubpartitionInfo, BufferBuilder> {
        AtomicInteger recycledMemorySegments;
        ResultPartition partition;

        @Override
        ResultSubpartitionInfo getInfo() {
            return new ResultSubpartitionInfo(0, 0);
        }

        @Override
        ResultSubpartitionRecoveredStateHandler createInstanceForTest() throws IOException {
            // given: Result partition with recycling counter.
            recycledMemorySegments = new AtomicInteger();
            partition =
                    new ResultPartitionBuilder()
                            .setNetworkBufferPool(
                                    new NetworkBufferPool(3, 32 * 1024) {
                                        @Override
                                        public void recycle(MemorySegment segment) {
                                            recycledMemorySegments.incrementAndGet();
                                            super.recycle(segment);
                                        }
                                    })
                            .build();
            partition.setup();

            return new ResultSubpartitionRecoveredStateHandler(
                    new ResultPartitionWriter[] {partition},
                    false,
                    new InflightDataRescalingDescriptor(
                            new int[] {1},
                            new RescaleMappings[] {RescaleMappings.identity(1, 1)},
                            new HashSet<>()));
        }

        @Override
        void assertState() {
            // Close the partition for flushing the cached recycled buffers to the segment provider.
            partition.close();

            // then: There are one internal memory segment(always present for subpartition) + one
            // buffer recycled from outside(if there is no consumer) and inside of 'recover'(if
            // there is the consumer).
            assertEquals(2, recycledMemorySegments.get());
        }
    }

    static class InputChannelRecoveredStateHandlerTester
            extends RecoveredChannelStateHandlerTester<InputChannelInfo, Buffer> {
        AtomicInteger recycledMemorySegments;
        SingleInputGate inputGate;

        @Override
        InputChannelInfo getInfo() {
            return new InputChannelInfo(0, 0);
        }

        @Override
        InputChannelRecoveredStateHandler createInstanceForTest() {
            // given: Segment provider with recycling counter.
            recycledMemorySegments = new AtomicInteger();
            InputChannelTestUtils.UnpooledMemorySegmentProvider segmentProvider =
                    new InputChannelTestUtils.UnpooledMemorySegmentProvider(32 * 1024) {
                        @Override
                        public void recycleMemorySegments(Collection<MemorySegment> segments) {
                            recycledMemorySegments.addAndGet(segments.size());
                        }
                    };

            // and: Configured input gate with recovered channel.
            inputGate =
                    new SingleInputGateBuilder()
                            .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                            .setSegmentProvider(segmentProvider)
                            .build();

            return new InputChannelRecoveredStateHandler(
                    new InputGate[] {inputGate},
                    new InflightDataRescalingDescriptor(
                            new int[] {1},
                            new RescaleMappings[] {RescaleMappings.identity(1, 1)},
                            new HashSet<>()));
        }

        @Override
        void assertState() throws Exception {
            // Close the partition for flushing the cached recycled buffers to the segment provider.
            inputGate.close();

            // then: There is only one recycled segments because 'recover' method should not recycle
            // the input buffer.
            assertEquals(1, recycledMemorySegments.get());
        }
    }

    /**
     * Helper class for testing different implementation of {@link RecoveredChannelStateHandler}.
     */
    abstract static class RecoveredChannelStateHandlerTester<Info, Context>
            implements RecoveredChannelStateHandler<Info, Context> {

        RecoveredChannelStateHandler<Info, Context> testTarget;

        @Override
        public BufferWithContext<Context> getBuffer(Info o)
                throws IOException, InterruptedException {
            return testTarget.getBuffer(o);
        }

        @Override
        public void recover(Info o, int oldSubtaskIndex, Context o2) throws IOException {
            testTarget.recover(o, oldSubtaskIndex, o2);
        }

        public void init() throws Exception {
            testTarget = createInstanceForTest();
        }

        @Override
        public void close() throws Exception {
            testTarget.close();
        }

        abstract Info getInfo();

        abstract RecoveredChannelStateHandler<Info, Context> createInstanceForTest()
                throws Exception;

        abstract void assertState() throws Exception;
    }
}
