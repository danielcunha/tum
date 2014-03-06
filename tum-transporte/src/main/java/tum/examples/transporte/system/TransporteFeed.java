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
	private static final List<String> types = new ArrayList<String>();

	public TransporteFeed() {
		types.addAll(Arrays.asList("Cadastro de trajeto", "Consulta por ônibus"));
	}

	@Override
	public void run() {
		while(true) {
			TumFeedEvent evt = new TumFeedEvent(System.currentTimeMillis(),
					"192.168.0."+(Math.round(Math.random()*254)),
					types.get((int) Math.round((Math.random()*1))));

			System.out.println(evt);
		}
	}

	public static final class TumFeedEvent {
		private final long time;
		private final String user;
		private final String type;


		public TumFeedEvent(Map<String, Object> jsonObject) {
			this((Long) jsonObject.get("time"), (String) jsonObject.get("user"), (String) jsonObject.get("type"));
		}

		public TumFeedEvent(Long time, String user, String type) {
			this.time = time;
			this.user = user;
			this.type = type;
		}


		public long getTime() {
			return time;
		}

		public String getUser() {
			return user;
		}

		public String getType() {
			return type;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TumFeedEvent that = (TumFeedEvent) o;

			if (time != that.time) return false;
			if (type != null ? !type.equals(that.type) : that.type != null) return false;
			if (user != null ? !user.equals(that.user) : that.user != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = (int) (time ^ (time >>> 32));
			result = 31 * result + (user != null ? user.hashCode() : 0);
			result = 31 * result + (type != null ? type.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "TumFeedEvent [time=" + time + ", user=" + user + ", type=" + type + "]";
		}

		public String toJson() {
			return toJson(this);
		}

		public static Map<String, Object> toMap(TumFeedEvent event) {
			Map<String, Object> jsonObject = new HashMap<String, Object>();

			jsonObject.put("time", event.getTime());
			jsonObject.put("user", event.getUser());
			jsonObject.put("type", event.getType());

			return jsonObject;
		}

		public static String toJson(TumFeedEvent event) {
			Map<String, Object> jsonObject = toMap(event);

			try {
				return jsonMapper.writeValueAsString(jsonObject);
			} catch (Exception e) {
				throw new SamzaException(e);
			}
		}

		@SuppressWarnings("unchecked")
		public static TumFeedEvent fromJson(String json) {
			try {
				return new TumFeedEvent((Map<String, Object>) jsonMapper.readValue(json, Map.class));
			} catch (Exception e) {
				throw new SamzaException(e);
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		TransporteFeed feed = new TransporteFeed();
		feed.start();


//		feed.listen(new TumFeedListener() {
//			@Override
//			public void onEvent(TumFeedEvent event) {
//				System.out.println(event);
//			}
//		});

		Thread.sleep(100);
	}
}
