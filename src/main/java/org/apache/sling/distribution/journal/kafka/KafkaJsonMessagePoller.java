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

import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;
import static java.lang.String.format;
import static java.time.Duration.ofHours;

public class KafkaJsonMessagePoller<T> implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonMessagePoller.class);

    private volatile boolean running = true;

    private final KafkaConsumer<String, String> consumer;

    private final MessageHandler<T> handler;

    private final ObjectReader reader;

    public KafkaJsonMessagePoller(KafkaConsumer<String, String> consumer, MessageHandler<T> handler, Class<T> clazz) {
        this.consumer = consumer;
        this.handler = handler;
        ObjectMapper mapper = new ObjectMapper();
        reader = mapper.readerFor(clazz);
        startBackgroundThread(this::run, format("Message Json Poller for handler %s", handler));
    }

    @Override
    public void close() throws IOException {
        LOG.info("Shutdown JSON poller for handler {}", handler);
        running = false;
        consumer.wakeup();
    }

    public void run() {
        LOG.info("Start JSON poller for handler {}", handler);
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
        LOG.info("Stop JSON poller for handler {}", handler);
    }

    private void consume() {
        consumer.poll(ofHours(1))
                .forEach(this::handleRecord);
    }

    private void handleRecord(ConsumerRecord<String, String> record) {
        MessageInfo info = new KafkaMessageInfo(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp());
        String payload = record.value();
        try {
            T message = reader.readValue(payload);
            handler.handle(info, message);
        } catch (IOException e) {
            LOG.error("Failed to parse payload {}", payload);
        }
    }
}
