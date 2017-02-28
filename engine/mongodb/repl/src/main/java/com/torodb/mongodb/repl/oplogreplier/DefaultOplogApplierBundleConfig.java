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

package com.torodb.mongodb.repl.oplogreplier;

import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.ReplCoreBundle;
import com.torodb.mongodb.repl.commands.ReplCommandExecutor;
import com.torodb.mongodb.repl.commands.ReplCommandLibrary;
import com.torodb.mongodb.repl.guice.ReplEssentialOverrideModule;

public class DefaultOplogApplierBundleConfig implements BundleConfig {

  private final ReplCoreBundle replCoreBundle;
  private final MongoDbCoreBundle mongoDbCorebundle;
  private final ReplCommandLibrary replCommandsLibrary;
  private final ReplCommandExecutor replCommandsExecutor;
  private final ReplEssentialOverrideModule essentialOverrideModule;
  private final BundleConfig delegate;

  public DefaultOplogApplierBundleConfig(ReplCoreBundle replCoreBundle,
      MongoDbCoreBundle mongoDbCorebundle, ReplCommandLibrary replCommandsLibrary,
      ReplCommandExecutor replCommandsExecutor, ReplEssentialOverrideModule essentialOverrideModule,
      BundleConfig delegate) {
    this.replCoreBundle = replCoreBundle;
    this.mongoDbCorebundle = mongoDbCorebundle;
    this.replCommandsLibrary = replCommandsLibrary;
    this.replCommandsExecutor = replCommandsExecutor;
    this.essentialOverrideModule = essentialOverrideModule;
    this.delegate = delegate;
  }

  public ReplCoreBundle getReplCoreBundle() {
    return replCoreBundle;
  }

  public MongoDbCoreBundle getMongoDbCoreBundle() {
    return mongoDbCorebundle;
  }

  public ReplCommandLibrary getReplCommandsLibrary() {
    return replCommandsLibrary;
  }

  public ReplCommandExecutor getReplCommandsExecutor() {
    return replCommandsExecutor;
  }

  public ReplEssentialOverrideModule getEssentialOverrideModule() {
    return essentialOverrideModule;
  }

  @Override
  public Injector getEssentialInjector() {
    return delegate.getEssentialInjector();
  }

  @Override
  public Supervisor getSupervisor() {
    return delegate.getSupervisor();
  }

}
