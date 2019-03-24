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
package org.apache.sling.distribution.journal.kafka.util;

import static org.osgi.util.converter.Converters.standardConverter;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.kafka.KafkaClientProvider;
import org.apache.sling.distribution.journal.kafka.KafkaEndpoint;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.google.common.collect.ImmutableMap;

public class KafkaRule implements TestRule {

    private KafkaClientProvider provider;

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                runWithKafka(base);
            }
        };
    }

    private void runWithKafka(Statement base) throws Throwable, IOException, Exception {
        try (KafkaLocal kafka = new KafkaLocal()) {
            this.provider = createProvider();
            base.evaluate();
            IOUtils.closeQuietly(this.provider);
        }
    }

    private KafkaClientProvider createProvider() {
        KafkaClientProvider provider = new KafkaClientProvider();
        ImmutableMap<String, String> props = ImmutableMap.of(
                "connectTimeout", "5000");
        KafkaEndpoint config = standardConverter().convert(props).to(KafkaEndpoint.class);
        provider.activate(config);
        return provider;
    }
    
    public MessagingProvider getProvider() {
        if (this.provider == null) {
            this.provider = createProvider();
        }
        return provider;
    }
}
