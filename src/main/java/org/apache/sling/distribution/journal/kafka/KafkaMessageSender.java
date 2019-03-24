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

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.sling.distribution.journal.messages.Types;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingException;
import com.google.protobuf.GeneratedMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.sling.distribution.journal.kafka.KafkaClientProvider.PARTITION;

public class KafkaMessageSender<T extends GeneratedMessage> implements MessageSender<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageSender.class);

    private final KafkaProducer<String, byte[]> producer;

    public KafkaMessageSender(KafkaProducer<String, byte[]> producer) {
        this.producer = requireNonNull(producer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void send(String topic, T payload) {
        Integer type = Types.getType(payload.getClass());
        if (type == null) {
            throw new IllegalArgumentException("No mapping for type " + payload.getClass().getName());
        }
        int version = Types.getVersion(payload.getClass());
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, PARTITION, null, null, payload.toByteArray(), toHeaders(type, version));
        try {
            RecordMetadata metadata = producer.send(record).get();
            LOG.info(format("Sent to %s", metadata));
        } catch (InterruptedException | ExecutionException e) {
            throw new MessagingException(format("Failed to send message on topic %s", topic), e);
        }
    }

    private Iterable<Header> toHeaders(int type, int version) {
        return Arrays.asList(toHeader("type", type),
                toHeader("version",version));
    }

    private Header toHeader(String key, int value) {
        return new RecordHeader(key, Integer.toString(value).getBytes(UTF_8));
    }
}
