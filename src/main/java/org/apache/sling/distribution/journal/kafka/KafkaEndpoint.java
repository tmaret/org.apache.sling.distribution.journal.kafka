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

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(name = "Apache Sling Journal Distribution - Kafka endpoint",
        description = "Apache Kafka Endpoint")
public @interface KafkaEndpoint {

    @AttributeDefinition(name = "Kafka Bootstrap Servers",
            description = "A comma separated list of host/port pairs to use for establishing the initial connection to the Kafka cluster.")
    String kafkaBootstrapServers() default "localhost:9092";

    @AttributeDefinition(name = "Kafka Request Timeout",
            description = "Kafka Request Timeout in ms.")
    int kafkaRequestTimeout() default 32000;

    @AttributeDefinition(name = "Kafka Default API Timeout",
            description = "Kafka Default API Timeout in ms.")
    int kafkaDefaultApiTimeout() default 60000;
}
