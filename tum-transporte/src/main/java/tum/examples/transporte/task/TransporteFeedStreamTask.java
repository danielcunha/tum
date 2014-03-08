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

package tum.examples.transporte.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import tum.examples.transporte.system.TransporteFeedEvent;

import java.util.Map;

/**
 * This task is very simple. All it does is take messages that it receives, and
 * sends them to a Kafka topic called transporte-raw.
 */
public class TransporteFeedStreamTask implements StreamTask {
	private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "transporte-raw");

	@Override
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		Map<String, Object> outgoingMap = TransporteFeedEvent.toMap((TransporteFeedEvent) envelope.getMessage());
		collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingMap));
	}
}
