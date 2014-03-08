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

package tum.examples.transporte.system;

import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;

public class TransporteConsumer extends BlockingEnvelopeMap implements TransporteFeed.TransporteFeedListener {
	private final String systemName;
	private final TransporteFeed feed;

	public TransporteConsumer(String systemName, TransporteFeed feed, MetricsRegistry registry) {
		this.systemName = systemName;
		this.feed = feed;
	}


	@Override
	public void register(SystemStreamPartition systemStreamPartition, String startingOffset) {
		super.register(systemStreamPartition, startingOffset);
	}

	@Override
	public void start() {
		feed.listen(this);
		feed.start();
	}

	@Override
	public void stop() {
		feed.unlisten(this);
		feed.interrupt();
	}

	@Override
	public void onEvent(TransporteFeedEvent event) {
		SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, "transporte.users", new Partition(0));

		try {
			put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, null, null, event));
		} catch (Exception e) {
			System.err.println(e);
		}
	}
}
