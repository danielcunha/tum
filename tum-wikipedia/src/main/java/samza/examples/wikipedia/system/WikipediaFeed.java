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

package samza.examples.wikipedia.system;

import org.apache.samza.SamzaException;
import org.codehaus.jackson.map.ObjectMapper;
import org.schwering.irc.lib.IRCEventListener;
import org.schwering.irc.lib.IRCModeParser;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WikipediaFeed {
	private static final Logger log = LoggerFactory.getLogger(WikipediaFeed.class);
	private static final Random random = new Random();
	private static final ObjectMapper jsonMapper = new ObjectMapper();

	private final Set<WikipediaFeedListener> listeners;


	public WikipediaFeed() {
		this.listeners =  new HashSet<WikipediaFeedListener>();
	}

	public void start() {
	}

	public void stop() {
	}

	public void listen(WikipediaFeedListener listener) {
		listeners.add(listener);
	}

	public void unlisten(String channel, WikipediaFeedListener listener) {
		if (listeners == null) {
			throw new RuntimeException("Trying to unlisten to a channel that has no listeners in it.");
		} else if (!listeners.contains(listener)) {
			throw new RuntimeException("Trying to unlisten to a channel that listener is not listening to.");
		}

		listeners.remove(listener);
	}

	/*public class WikipediaFeedIrcListener implements IRCEventListener {
		public void onPrivmsg(String chan, IRCUser u, String msg) {
			Set<WikipediaFeedListener> listeners = mapListeners.get(chan);

			if (listeners != null) {
				WikipediaFeedEvent event = new WikipediaFeedEvent(System.currentTimeMillis(), chan, u.getNick(), msg);

				for (WikipediaFeedListener listener : listeners) {
					listener.onEvent(event);
				}
			}

			log.debug(chan + "> " + u.getNick() + ": " + msg);
		}
	}*/

	public static interface WikipediaFeedListener {
		void onEvent(WikipediaFeedEvent event);
	}

	public static final class WikipediaFeedEvent {
		private final long time;
		private final String user;
		private final String type;


		public WikipediaFeedEvent(Map<String, Object> jsonObject) {
			this((Long) jsonObject.get("time"), (String) jsonObject.get("user"), (String) jsonObject.get("type"));
		}

		public WikipediaFeedEvent(Long time, String user, String type) {
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

			WikipediaFeedEvent that = (WikipediaFeedEvent) o;

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
			return "WikipediaFeedEvent [time=" + time + ", user=" + user + ", type=" + type + "]";
		}

		public String toJson() {
			return toJson(this);
		}

		public static Map<String, Object> toMap(WikipediaFeedEvent event) {
			Map<String, Object> jsonObject = new HashMap<String, Object>();

			jsonObject.put("time", event.getTime());
			jsonObject.put("user", event.getUser());
			jsonObject.put("type", event.getType());

			return jsonObject;
		}

		public static String toJson(WikipediaFeedEvent event) {
			Map<String, Object> jsonObject = toMap(event);

			try {
				return jsonMapper.writeValueAsString(jsonObject);
			} catch (Exception e) {
				throw new SamzaException(e);
			}
		}

		@SuppressWarnings("unchecked")
		public static WikipediaFeedEvent fromJson(String json) {
			try {
				return new WikipediaFeedEvent((Map<String, Object>) jsonMapper.readValue(json, Map.class));
			} catch (Exception e) {
				throw new SamzaException(e);
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		WikipediaFeed feed = new WikipediaFeed();
		feed.start();

		feed.listen(new WikipediaFeedListener() {
			@Override
			public void onEvent(WikipediaFeedEvent event) {
				System.out.println(event);
			}
		});

		Thread.sleep(20000);
		feed.stop();
	}
}
