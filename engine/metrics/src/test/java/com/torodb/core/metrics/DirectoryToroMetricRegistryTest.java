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

package com.torodb.core.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.metrics.directory.Directory;
import com.torodb.core.metrics.directory.RootMetricDirectory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class DirectoryToroMetricRegistryTest {

  @Parameter(0)
  public String ignored;
  @Parameter(1)
  public Directory directory;
  private SafeRegistry safeRegistry;
  private DirectoryToroMetricRegistry registry;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return ImmutableMap.builder()
        .put("root", new RootMetricDirectory())
        .put("defaultKey", new RootMetricDirectory().createDirectory("aValue"))
        .put("custom", new RootMetricDirectory().createDirectory("aKey", "aValye"))
        .build()
        .entrySet()
        .stream()
        .map(entry -> new Object[] {entry.getKey(), entry.getValue()})
        .collect(Collectors.toList());
  }

  @Before
  public void setUp() {
    directory = new RootMetricDirectory();
    safeRegistry = spy(new AdaptorSafeRegistry(new MetricRegistry()));
    registry = new DirectoryToroMetricRegistry(
        directory,
        safeRegistry
    );
  }
  
  @Test
  public void testCreateSubRegistry_2_ok() {
    ToroMetricRegistry subRegistry = registry.createSubRegistry("akey", "avalue");

    String meterName = "aName";
    Meter meter = registry.meter(meterName);
    Meter subMeter = subRegistry.meter(meterName);

    assertThat(meter, is(not(subMeter)));
  }

  @Test
  public void testCreateSubRegistry_default_ok() {
    ToroMetricRegistry subRegistry = registry.createSubRegistry("avalue");

    String meterName = "aName";
    Meter meter = registry.meter(meterName);
    Meter subMeter = subRegistry.meter(meterName);

    assertThat(meter, is(not(subMeter)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSubRegistry_2_illegalKey() {
    registry.createSubRegistry("a-illegal-key", "avalue");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSubRegistry_2_illegalValue() {
    registry.createSubRegistry("aValue", "a-illegal-value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSubRegistry_default_illegalValue() {
    registry.createSubRegistry("a-illegal-value");
  }

  @Test
  public void testCounter() {
    String sName = "legalName";
    Name name = directory.createName(sName);

    Counter metric = registry.counter(sName);

    verify(safeRegistry).counter(name);

    assertThat(metric, is(safeRegistry.counter(name)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalCounter() {
    String sName = "illegal-Name";
    registry.counter(sName);
  }

  @Test
  public void testMeter() {
    String sName = "legalName";
    Name name = directory.createName(sName);

    Meter metric = registry.meter(sName);

    verify(safeRegistry).meter(name);

    assertThat(metric, is(safeRegistry.meter(name)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalMeter() {
    String sName = "illegal-Name";
    registry.meter(sName);
  }

  @Test
  public void testHistogram() {
    String sName = "legalName";
    Name name = directory.createName(sName);

    Histogram metric = registry.histogram(sName);

    verify(safeRegistry).histogram(name, false);

    assertThat(metric, is(safeRegistry.histogram(name)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalHistogram() {
    String sName = "illegal-Name";
    registry.histogram(sName);
  }

  @Test
  public void testTimer() {
    String sName = "legalName";
    Name name = directory.createName(sName);

    Timer metric = registry.timer(sName);

    verify(safeRegistry).timer(name, false);

    assertThat(metric, is(safeRegistry.timer(name)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalTimer() {
    String sName = "illegal-Name";
    registry.timer(sName);
  }

  @Test
  public void testGauge() {
    String sName = "legalName";
    Name name = directory.createName(sName);

    SettableGauge<?> metric = registry.gauge(sName);

    verify(safeRegistry).gauge(name);

    assertThat(metric, is(safeRegistry.gauge(name)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalGauge() {
    String sName = "illegal-Name";
    registry.gauge(sName);
  }

  @Test
  public void testRegister() {
    String sName = "legalName";
    Name name = directory.createName(sName);

    Counter actualCounter = new Counter();
    Counter metric = registry.register(sName, actualCounter);

    verify(safeRegistry).register(name, actualCounter);

    assertThat(metric, is(safeRegistry.register(name, actualCounter)));
    assertThat(metric, is(safeRegistry.counter(name)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalRegister() {
    String sName = "illegal-Name";
    registry.register(sName, new Counter());
  }
  
  @Test
  public void testRemove() {
    String sName = "legalName";
    Name name = directory.createName(sName);

    registry.remove(sName);

    verify(safeRegistry).remove(name);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalRemove() {
    String sName = "illegal-Name";
    registry.remove(sName);
  }

}
