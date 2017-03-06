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

package com.torodb.core.guice;

import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.metrics.ToroMetricRegistry;

/**
 * A {@link PrivateModule} on which common (aka non essential) ToroDB modules delegates the binding
 * of essential classes.
 *
 * <p/>It can be subclassed so some classes are bound as desired. For example, if a bundle decides
 * to use a different logger, it can subclass this class overriding {@link #bindLoggerFactory() } 
 * to what is required.
 */
public class EssentialToDefaultModule extends PrivateModule {

  @Override
  protected void configure() {
    expose(ToroMetricRegistry.class);
    expose(LoggerFactory.class);

    bindToroMetricRegistry();
    bindLoggerFactory();
  }

  protected <T> Key<T> getEssentialKey(Class<T> clazz) {
    return Key.get(clazz, Essential.class);
  }

  private <T> void bindEssentialAsDefault(Class<T> clazz) {
    bind(clazz)
        .to(getEssentialKey(clazz));
  }

  protected void bindLoggerFactory() {
    bindEssentialAsDefault(LoggerFactory.class);
  }

  protected void bindToroMetricRegistry() {
    bindEssentialAsDefault(ToroMetricRegistry.class);
  }
}
