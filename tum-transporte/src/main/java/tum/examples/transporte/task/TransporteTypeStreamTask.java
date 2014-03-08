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

import java.util.*;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

public class TransporteTypeStreamTask implements StreamTask, WindowableTask {

	private int evts = 0;
	private Map<String, List<Object>> groups = new HashMap<String, List<Object>>();
	private Map<String, Integer> counts = new HashMap<String, Integer>();

	@SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

    Map<String, Object> evt = (Map<String, Object>) envelope.getMessage();
	List<Object> evts = null;

	if(counts.containsKey(evt.get("type"))) {
		evts = ((List) groups.get(evt.get("type")));
	} else {
		evts = new ArrayList<Object>();
	}

	evts.add(evt);
	groups.put((String) evt.get("type"), evts);
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
	for(String key : groups.keySet()) {
		counts.put(key, groups.get(key).size());
	}
	counts.put("total", evts);

	collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "transporte-types-counts"), counts));
	collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "transporte-types-groups"), groups));

    // Reset groups after windowing.
	 evts = 0;
	groups = new HashMap<String, List<Object>>();
	counts = new HashMap<String, Integer>();
  }
}
