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

package com.torodb.engine.essential;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.torodb.core.metrics.MetricsConfig;

import java.time.Clock;

public class EssentialInjectorFactory {

  private EssentialInjectorFactory() {}

  public static Injector createEssentialInjector() {
    return createEssentialInjector(() -> true, Clock.systemUTC());
  }

  public static Injector createEssentialInjector(MetricsConfig metricsConfig, Clock clock) {
    return createEssentialInjector(metricsConfig, clock, Stage.PRODUCTION);
  }

  public static Injector createEssentialInjector(
      MetricsConfig metricsConfig,
      Clock clock,
      Stage stage) {
    return Guice.createInjector(stage, new EssentialModule(metricsConfig, clock));
  }

}
