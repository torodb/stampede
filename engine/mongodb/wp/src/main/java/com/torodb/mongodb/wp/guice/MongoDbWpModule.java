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

package com.torodb.mongodb.wp.guice;

import com.eightkdata.mongowp.bson.netty.NettyStringReader;
import com.eightkdata.mongowp.bson.netty.PooledNettyStringReader;
import com.eightkdata.mongowp.bson.netty.pool.OnlyLikelyStringPoolPolicy;
import com.eightkdata.mongowp.bson.netty.pool.ShortStringPoolPolicy;
import com.eightkdata.mongowp.bson.netty.pool.StringPool;
import com.eightkdata.mongowp.bson.netty.pool.WeakMapStringPool;
import com.eightkdata.mongowp.server.api.ErrorHandler;
import com.eightkdata.mongowp.server.api.RequestProcessorAdaptor;
import com.eightkdata.mongowp.server.callback.RequestProcessor;
import com.eightkdata.mongowp.server.wp.DefaultRequestIdGenerator;
import com.eightkdata.mongowp.server.wp.RequestIdGenerator;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.mongodb.core.ToroErrorHandler;
import com.torodb.mongodb.wp.TorodbSafeRequestProcessor;

/**
 *
 */
public class MongoDbWpModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(NettyStringReader.class)
        .to(PooledNettyStringReader.class)
        .in(Singleton.class);

    configureStringPool();

    bind(RequestIdGenerator.class)
        .to(DefaultRequestIdGenerator.class);

    bind(ErrorHandler.class)
        .to(ToroErrorHandler.class)
        .in(Singleton.class);
  }

  private void configureStringPool() {
    bind(StringPool.class)
        //                .toInstance(new InternStringPool(
        //                        OnlyLikelyStringPoolPolicy.getInstance()
        //                                .or(new ShortStringPoolPolicy(5))
        //                ));

        //                .toInstance(new GuavaStringPool(
        //                        OnlyLikelyStringPoolPolicy.getInstance()
        //                                .or(new ShortStringPoolPolicy(5)),
        //                        CacheBuilder.newBuilder().maximumSize(100_000)
        //                ));
        .toInstance(new WeakMapStringPool(
            OnlyLikelyStringPoolPolicy.getInstance()
                .or(new ShortStringPoolPolicy(5))
        ));
  }

  @Provides
  RequestProcessor createRequestProcessorAdaptor(TorodbSafeRequestProcessor tsrp,
      ErrorHandler errorHandler) {
    return new RequestProcessorAdaptor<>(tsrp, errorHandler);
  }

}
