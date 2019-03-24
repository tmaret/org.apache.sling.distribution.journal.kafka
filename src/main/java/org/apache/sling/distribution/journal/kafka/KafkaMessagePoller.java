/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.distribution.journal.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.distribution.journal.messages.Types;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import com.google.protobuf.ByteString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofHours;

public class KafkaMessagePoller implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessagePoller.class);

    private final Map<Class<?>, HandlerAdapter<?>> handlers = new HashMap<>();

    private final KafkaConsumer<String, byte[]> consumer;

    private volatile boolean running = true;

    private final String types;

    public KafkaMessagePoller(KafkaConsumer<String, byte[]> consumer, HandlerAdapter<?>... handlerAdapters) {
        this.consumer = consumer;
        for (HandlerAdapter<?> handlerAdapter : handlerAdapters) {
            handlers.put(handlerAdapter.getType(), handlerAdapter);
        }
        types = handlers.keySet().toString();
        startBackgroundThread(this::run, format("Message Poller %s", types));
    }

    @Override
    public void close() throws IOException {
        LOG.info("Shutdown poller for types {}", types);
        running = false;
        consumer.wakeup();
    }

    public void run() {
        LOG.info("Start poller for types {}", types);
        try {
            while(running) {
                consume();
            }
        } catch (WakeupException e) {
            if (running) {
                LOG.error("Waked up while running {}", e.getMessage(), e);
                throw e;
            } else {
                LOG.debug("Waked up while stopping {}", e.getMessage(), e);
            }
        } catch(Throwable t) {
            LOG.error(format("Catch Throwable %s closing consumer", t.getMessage()), t);
            throw t;
        } finally {
            consumer.close();
        }
        LOG.info("Stop poller for types {}", types);
    }

    private void consume() {
        consumer.poll(ofHours(1))
                .forEach(this::handleRecord);
    }

    private void handleRecord(ConsumerRecord<String, byte[]> record) {
        Class<?> type = Types.getType(
                parseInt(getHeaderValue(record.headers(), "type")),
                parseInt(getHeaderValue(record.headers(), "version")));
        HandlerAdapter<?> adapter = handlers.get(type);
        if (adapter != null) {
            try {
                handleRecord(adapter, record);
            } catch (Exception e) {
                String msg = format("Error consuming message for types %s", types);
                LOG.warn(msg);
            }
        } else {
            LOG.debug("No handler registered for type {}", type.getName());
        }
    }

    private void handleRecord(HandlerAdapter<?> adapter, ConsumerRecord<String, byte[]> record) throws Exception {
        MessageInfo info = new KafkaMessageInfo(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp());
        ByteString payload = ByteString.copyFrom(record.value());
        adapter.handle(info, payload);
    }

    private String getHeaderValue(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        if (header == null) {
            throw new IllegalArgumentException(format("Header with key %s not found", key));
        }
        return new String(header.value(), UTF_8);
    }
}
