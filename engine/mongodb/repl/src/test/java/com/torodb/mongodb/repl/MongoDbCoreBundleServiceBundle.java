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

import com.google.common.util.concurrent.Service;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.modules.ServiceBundle;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.core.MongoDbCoreConfig;
import com.torodb.torod.MemoryTorodBundle;
import com.torodb.torod.TorodBundle;

import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class MongoDbCoreBundleServiceBundle extends ServiceBundle<MongoDbCoreBundle>{

  private final TorodBundle torodBundle;
  private final MongoDbCoreBundle mongoDbCoreBundle;

  public static MongoDbCoreBundleServiceBundle createBundle() {
    TestBundleConfig generalConfig = new TestBundleConfig();
    return new MongoDbCoreBundleServiceBundle(generalConfig);
  }

  public MongoDbCoreBundleServiceBundle(BundleConfig generalConfig) {
    super(generalConfig);

    torodBundle = new MemoryTorodBundle(generalConfig);

    MongoDbCoreConfig mongoDbCoreConfig = MongoDbCoreConfig.simpleNonServerConfig(torodBundle,
        generalConfig);

    mongoDbCoreBundle = new MongoDbCoreBundle(mongoDbCoreConfig);
  }

  @Override
  protected List<Service> getManagedDependencies() {
    return Collections.singletonList(torodBundle);
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  public MongoDbCoreBundle getExternalInterface() {
    return mongoDbCoreBundle;
  }

}
