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

package com.torodb.backend.postgresql;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.torodb.core.metrics.MetricNameFactory;
import com.torodb.core.metrics.ToroMetricRegistry;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public final class PostgreSqlMetrics {

  private final Timer insertDocPartDataTimer;
  private final Meter insertRows;
  private final Meter insertFields;
  private final Meter insertDefault;
  private final Meter insertCopy;

  @Inject
  public PostgreSqlMetrics(ToroMetricRegistry registry, MetricNameFactory nameFactory) {
    MetricNameFactory myFactory = nameFactory.createSubFactory("PostgreSQLWrite");
    insertDocPartDataTimer = registry.timer(myFactory.createMetricName("insertDocPartDataTimer"));
    insertRows = registry.meter(myFactory.createMetricName("insertRows"));
    insertFields = registry.meter(myFactory.createMetricName("insertFields"));
    insertDefault = registry.meter(myFactory.createMetricName("insertDefault"));
    insertCopy = registry.meter(myFactory.createMetricName("insertCopy"));
  }

  public Timer getInsertDocPartDataTimer() {
    return insertDocPartDataTimer;
  }

  public Meter getInsertRows() {
    return insertRows;
  }

  public Meter getInsertFields() {
    return insertFields;
  }

  public Meter getInsertDefault() {
    return insertDefault;
  }

  public Meter getInsertCopy() {
    return insertCopy;
  }

}
