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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Queues;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.flink.util.Preconditions.checkArgument;

/** */
public class TestPooledBufferProvider implements BufferProvider {

    private final BlockingQueue<MemorySegment> buffers = new LinkedBlockingDeque<>();

    private final TestBufferFactory bufferFactory;

    private final PooledBufferProviderRecycler bufferRecycler;

    public TestPooledBufferProvider(int poolSize) {
        this(poolSize, 32 * 1024);
    }

    public TestPooledBufferProvider(int poolSize, int bufferSize) {
        checkArgument(poolSize > 0);

        this.bufferRecycler = new PooledBufferProviderRecycler(buffers);
        this.bufferFactory = new TestBufferFactory(poolSize, bufferSize, bufferRecycler);
    }

    @Override
    public Buffer requestBuffer() {
        MemorySegment memorySegment = requestMemorySegment();

        return memorySegment == null ? null : new NetworkBuffer(memorySegment, bufferRecycler);
    }

    @Override
    public BufferBuilder requestBufferBuilder() {
        Buffer buffer = requestBuffer();
        if (buffer != null) {
            return new BufferBuilder(buffer);
        }
        return null;
    }

    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        return requestBufferBuilder();
    }

    private Buffer requestBufferBlocking() throws InterruptedException {
        MemorySegment memorySegment = requestMemorySegmentBlocking();

        return memorySegment == null ? null : new NetworkBuffer(memorySegment, bufferRecycler);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        return new BufferBuilder(requestBufferBlocking());
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        return requestBufferBuilderBlocking();
    }

    @Override
    public boolean addBufferListener(BufferListener listener) {
        return bufferRecycler.registerListener(listener);
    }

    @Override
    public boolean isDestroyed() {
        return false;
    }

    @Override
    public MemorySegment requestMemorySegment() {
        final MemorySegment buffer = buffers.poll();
        if (buffer != null) {
            return buffer;
        }

        return bufferFactory.createMemorySegment();
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
        MemorySegment buffer = buffers.poll();
        if (buffer != null) {
            return buffer;
        }

        buffer = bufferFactory.createMemorySegment();
        if (buffer != null) {
            return buffer;
        }

        return buffers.take();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }

    public int getNumberOfAvailableBuffers() {
        return buffers.size();
    }

    public int getNumberOfCreatedBuffers() {
        return bufferFactory.getNumberOfCreatedBuffers();
    }

    private static class PooledBufferProviderRecycler implements BufferRecycler {

        private final Object listenerRegistrationLock = new Object();

        private final Queue<MemorySegment> buffers;

        private final ConcurrentLinkedQueue<BufferListener> registeredListeners =
                Queues.newConcurrentLinkedQueue();

        public PooledBufferProviderRecycler(Queue<MemorySegment> buffers) {
            this.buffers = buffers;
        }

        @Override
        public void recycle(MemorySegment segment) {
            synchronized (listenerRegistrationLock) {
                BufferListener listener = registeredListeners.poll();

                if (listener == null) {
                    buffers.add(segment);
                } else {
                    listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
                }
            }
        }

        boolean registerListener(BufferListener listener) {
            synchronized (listenerRegistrationLock) {
                if (buffers.isEmpty()) {
                    registeredListeners.add(listener);

                    return true;
                }

                return false;
            }
        }
    }
}
