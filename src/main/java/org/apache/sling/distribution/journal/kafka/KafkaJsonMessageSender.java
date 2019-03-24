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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.sling.distribution.journal.kafka.KafkaClientProvider.PARTITION;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.sling.distribution.journal.JsonMessageSender;
import org.apache.sling.distribution.journal.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class KafkaJsonMessageSender<T> implements JsonMessageSender<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonMessageSender.class);

    private final ObjectMapper mapper = new ObjectMapper();

    private final KafkaProducer<String, String> producer;

    public KafkaJsonMessageSender(KafkaProducer<String, String> producer) {
        this.producer = requireNonNull(producer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void send(String topic, T payload) {
        try {
            ObjectWriter writer = mapper.writerFor(payload.getClass());
            String payloadSt = writer.writeValueAsString(payload);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, PARTITION, null, payloadSt);
            RecordMetadata metadata = producer.send(record).get();
            LOG.info(format("Sent JSON to %s", metadata));
        } catch (Exception e) {
            throw new MessagingException(format("Failed to send JSON message on topic %s", topic), e);
        }
    }
}
