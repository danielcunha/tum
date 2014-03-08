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

import org.apache.samza.SamzaException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TransporteFeed extends Thread {
	private static final Logger log = LoggerFactory.getLogger(TransporteFeed.class);
	private static final Random random = new Random();
	private static final ObjectMapper jsonMapper = new ObjectMapper();

	private final Set<TransporteFeedListener> listeners;
	private static final List<String> types = new ArrayList<String>();

	public TransporteFeed() {
		this.listeners = new HashSet<TransporteFeedListener>();
		types.addAll(Arrays.asList("Cadastro de trajeto", "Consulta por ônibus"));
	}

	@Override
	public void run() {
		while(true) {
			TransporteFeedEvent evt = new TransporteFeedEvent(System.currentTimeMillis(),
					"192.168.0."+(Math.round(Math.random()*254)),
					types.get((int) Math.round((Math.random()*1))));

			for (TransporteFeedListener listener : listeners) {
				listener.onEvent(evt);
			}
		}
	}

	public void listen(TransporteFeedListener listener) {
		this.listeners.add(listener);
	}

	public void unlisten(TransporteFeedListener listener) {
		listeners.remove(listener);
	}

	public static interface TransporteFeedListener {
		void onEvent(TransporteFeedEvent event);
	}

	public static void main(String[] args) throws InterruptedException {
		TransporteFeed feed = new TransporteFeed();
		feed.start();

		feed.listen(new TransporteFeedListener() {
			@Override
			public void onEvent(TransporteFeedEvent event) {
				System.out.println(event);
			}
		});

		Thread.sleep(100);
//		feed.stop();
	}
}
