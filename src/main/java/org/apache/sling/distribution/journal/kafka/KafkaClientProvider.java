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
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.JsonMessageSender;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

@Component(service = MessagingProvider.class, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = KafkaEndpoint.class)
public class KafkaClientProvider implements MessagingProvider, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientProvider.class);

    public static final int PARTITION = 0;

    private volatile KafkaProducer<String, byte[]> rawProducer = null;

    private volatile KafkaProducer<String, String> jsonProducer = null;

    private String kafkaBootstrapServers;

    private int requestTimeout;

    private int defaultApiTimeout;

    @Activate
    public void activate(KafkaEndpoint kafkaEndpoint) {
        kafkaBootstrapServers = requireNonNull(kafkaEndpoint.kafkaBootstrapServers());
        requestTimeout = kafkaEndpoint.kafkaRequestTimeout();
        defaultApiTimeout = kafkaEndpoint.kafkaDefaultApiTimeout();
    }
    
    @Deactivate
    public void close() {
        IOUtils.closeQuietly(rawProducer);
        IOUtils.closeQuietly(jsonProducer);
    }

    @Override
    public <T extends GeneratedMessage> MessageSender<T> createSender() {
        return new KafkaMessageSender<>(buildKafkaProducer());
    }

    @Override
    public <T> Closeable createPoller(String topicName, Reset reset, HandlerAdapter<?>... adapters) {
        return createPoller(topicName, reset, null, adapters);
    }

    @Override
    public Closeable createPoller(String topicName, Reset reset, @Nullable String assign, HandlerAdapter<?>... adapters) {
        String consumerGroupId = UUID.randomUUID().toString();
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig(ByteArrayDeserializer.class, consumerGroupId, reset));
        TopicPartition topicPartition = new TopicPartition(topicName, PARTITION);
        Collection<TopicPartition> topicPartitions = singleton(topicPartition);
        consumer.assign(topicPartitions);
        if (assign != null) {
            consumer.seek(topicPartition, offset(assign));
        } else if (reset == Reset.earliest) {
            consumer.seekToBeginning(topicPartitions);
        } else {
            consumer.seekToEnd(topicPartitions);
        }
        Closeable poller = new KafkaMessagePoller(consumer, adapters);
        LOG.info(format("Created poller for consumerGroupId %s, reset %s, topicName %s, assign %s", consumerGroupId, reset, topicName, assign));
        return poller;
    }

    @Override
    public <T> JsonMessageSender<T> createJsonSender() {
        return new KafkaJsonMessageSender<>(buildJsonKafkaProducer());
    }

    @Override
    public <T> Closeable createJsonPoller(String topicName, Reset reset, MessageHandler<T> handler, Class<T> type) {
        String consumerGroupId = UUID.randomUUID().toString();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(StringDeserializer.class, consumerGroupId, reset));
        TopicPartition topicPartition = new TopicPartition(topicName, PARTITION);
        Collection<TopicPartition> topicPartitions = singleton(topicPartition);
        consumer.assign(topicPartitions);
        if (reset == Reset.earliest) {
            consumer.seekToBeginning(topicPartitions);
        } else {
            consumer.seekToEnd(topicPartitions);
        }
        return new KafkaJsonMessagePoller<>(consumer, handler, type);
    }

    @Override
    public void assertTopic(String topic) throws MessagingException {
        String consumerGroupId = UUID.randomUUID().toString();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(StringDeserializer.class, consumerGroupId, Reset.latest))) {
            if (! consumer.listTopics().containsKey(topic)) {
                throw new MessagingException(format("Topic %s does not exist", topic));
            }
        } catch (Exception e) {
            throw new MessagingException(format("Unable to load topic stats for %s", topic), e);
        }
    }

    @Override
    public long retrieveOffset(String topicName, Reset reset) {
        String groupId = UUID.randomUUID().toString();
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig(ByteArrayDeserializer.class, groupId, reset));
        TopicPartition topicPartition = new TopicPartition(topicName, PARTITION);
        Collection<TopicPartition> topicPartitions = singleton(topicPartition);
        final Map<TopicPartition, Long> offsets;
        if (reset == Reset.earliest) {
            offsets = consumer.beginningOffsets(topicPartitions);
        } else {
            offsets = consumer.endOffsets(topicPartitions);
        }
        consumer.close();
        return offsets.get(topicPartition);
    }

    @Override
    public String assignTo(long offset) {
        return format("%s:%s", PARTITION, offset);
    }

    @Nonnull
    private synchronized KafkaProducer<String, byte[]> buildKafkaProducer() {
        if (rawProducer == null) {
            rawProducer = new KafkaProducer<>(producerConfig(ByteArraySerializer.class));
        }
        return rawProducer;
    }

    @Nonnull
    private synchronized KafkaProducer<String, String> buildJsonKafkaProducer() {
        if (jsonProducer == null) {
            jsonProducer = new KafkaProducer<>(producerConfig(StringSerializer.class));
        }
        return jsonProducer;
    }

    private Map<String, Object> consumerConfig(Object deserializer, String consumerGroupId, Reset reset) {
        Map<String, Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(GROUP_ID_CONFIG, consumerGroupId);
        config.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeout);
        config.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        config.put(AUTO_OFFSET_RESET_CONFIG, reset.name());
        return config;
    }

    private Map<String, Object> producerConfig(Object serializer) {
        Map<String, Object> config = new HashMap<>();
        config.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        config.put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        config.put(ACKS_CONFIG, "all");
        return unmodifiableMap(config);
    }

    private long offset(String assign) {
        String[] chunks = assign.split(":");
        if (chunks.length != 2) {
            throw new IllegalArgumentException(format("Illegal assign %s", assign));
        }
        return Long.parseLong(chunks[1]);
    }


}
