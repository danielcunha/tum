package tum.examples.transporte.system;

import org.apache.samza.SamzaException;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by daniel on 07/03/14.
 */
public class TransporteFeedEvent {

	private final long time;
	private final String user;
	private final String type;
	private static final ObjectMapper jsonMapper = new ObjectMapper();

	public TransporteFeedEvent(Map<String, Object> jsonObject) {
		this((Long) jsonObject.get("time"), (String) jsonObject.get("user"), (String) jsonObject.get("type"));
	}

	public TransporteFeedEvent(Long time, String user, String type) {
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

		TransporteFeedEvent that = (TransporteFeedEvent) o;

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
		return "TransporteFeedEvent [time=" + time + ", user=" + user + ", type=" + type + "]";
	}

	public String toJson() {
		return toJson(this);
	}

	public static Map<String, Object> toMap(TransporteFeedEvent event) {
		Map<String, Object> jsonObject = new HashMap<String, Object>();

		jsonObject.put("time", event.getTime());
		jsonObject.put("user", event.getUser());
		jsonObject.put("type", event.getType());

		return jsonObject;
	}

	public static String toJson(TransporteFeedEvent event) {
		Map<String, Object> jsonObject = toMap(event);

		try {
			return jsonMapper.writeValueAsString(jsonObject);
		} catch (Exception e) {
			throw new SamzaException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static TransporteFeedEvent fromJson(String json) {
		try {
			return new TransporteFeedEvent((Map<String, Object>) jsonMapper.readValue(json, Map.class));
		} catch (Exception e) {
			throw new SamzaException(e);
		}
	}
}