/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.repl;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.torodb.core.metrics.MetricNameFactory;
import com.torodb.core.metrics.SettableGauge;
import com.torodb.core.metrics.ToroMetricRegistry;
import com.torodb.mongodb.commands.pojos.MemberState;

import java.util.Locale;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class ReplMetrics {

  private final SettableGauge<String> memberState;
  private final ImmutableMap<MemberState, Counter> memberStateCounters;
  private final SettableGauge<String> lastOpTimeFetched;
  private final SettableGauge<String> lastOpTimeApplied;

  @Inject
  public ReplMetrics(ToroMetricRegistry registry, MetricNameFactory parentNameFactory) {
    MetricNameFactory myFactory = parentNameFactory.createSubFactory("Repl");
    memberState = registry.gauge(myFactory.createMetricName("currentMemberState"));
    ImmutableMap.Builder<MemberState, Counter> memberStateCountersBuilder =
        ImmutableMap.builder();
    for (MemberState memberState : MemberState.values()) {
      memberStateCountersBuilder.put(memberState,
          registry.counter(myFactory.createMetricName(
              memberState.name().substring(3).toLowerCase(Locale.US) + "Count")));
    }
    memberStateCounters = Maps.immutableEnumMap(memberStateCountersBuilder.build());
    lastOpTimeFetched = registry.gauge(myFactory.createMetricName("lastOpTimeFetched"));
    lastOpTimeApplied = registry.gauge(myFactory.createMetricName("lastOpTimeApplied"));
  }

  public SettableGauge<String> getMemberState() {
    return memberState;
  }

  public ImmutableMap<MemberState, Counter> getMemberStateCounters() {
    return memberStateCounters;
  }

  public SettableGauge<String> getLastOpTimeFetched() {
    return lastOpTimeFetched;
  }

  public SettableGauge<String> getLastOpTimeApplied() {
    return lastOpTimeApplied;
  }
}
