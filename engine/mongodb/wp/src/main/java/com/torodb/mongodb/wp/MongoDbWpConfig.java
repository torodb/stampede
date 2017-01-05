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

package com.torodb.mongodb.wp;

import com.google.inject.Injector;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.supervision.Supervisor;
import com.torodb.torod.TorodBundle;

import java.util.concurrent.ThreadFactory;

/**
 * The configuration used by {@link MongoDbWpBundle}.
 */
public class MongoDbWpConfig implements BundleConfig {
  private final TorodBundle torodBundle;
  private final int port;
  private final BundleConfig delegate;

  @SuppressWarnings("checkstyle:JavadocMethod")
  public MongoDbWpConfig(TorodBundle torodBundle, int port, BundleConfig delegate) {
    this.torodBundle = torodBundle;
    this.port = port;
    this.delegate = delegate;
  }

  public TorodBundle getTorodBundle() {
    return torodBundle;
  }

  public int getPort() {
    return port;
  }

  @Override
  public Injector getEssentialInjector() {
    return delegate.getEssentialInjector();
  }

  @Override
  public ThreadFactory getThreadFactory() {
    return delegate.getThreadFactory();
  }

  @Override
  public Supervisor getSupervisor() {
    return delegate.getSupervisor();
  }

}
