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

import com.eightkdata.mongowp.bson.netty.DefaultNettyBsonLowLevelReader;
import com.eightkdata.mongowp.bson.netty.NettyBsonDocumentReader;
import com.eightkdata.mongowp.bson.netty.NettyBsonDocumentWriter;
import com.eightkdata.mongowp.bson.netty.NettyStringReader;
import com.eightkdata.mongowp.bson.netty.OffHeapNettyBsonLowLevelReader;
import com.eightkdata.mongowp.bson.netty.OffHeapValuesNettyBsonLowLevelReader;
import com.eightkdata.mongowp.bson.netty.PooledNettyStringReader;
import com.eightkdata.mongowp.bson.netty.pool.OnlyLikelyStringPoolPolicy;
import com.eightkdata.mongowp.bson.netty.pool.ShortStringPoolPolicy;
import com.eightkdata.mongowp.bson.netty.pool.StringPool;
import com.eightkdata.mongowp.bson.netty.pool.WeakMapStringPool;
import com.eightkdata.mongowp.server.MongoServerConfig;
import com.eightkdata.mongowp.server.api.ErrorHandler;
import com.eightkdata.mongowp.server.api.RequestProcessorAdaptor;
import com.eightkdata.mongowp.server.callback.RequestProcessor;
import com.eightkdata.mongowp.server.decoder.DeleteMessageDecoder;
import com.eightkdata.mongowp.server.decoder.GetMoreMessageDecoder;
import com.eightkdata.mongowp.server.decoder.InsertMessageDecoder;
import com.eightkdata.mongowp.server.decoder.KillCursorsMessageDecoder;
import com.eightkdata.mongowp.server.decoder.MessageDecoderLocator;
import com.eightkdata.mongowp.server.decoder.QueryMessageDecoder;
import com.eightkdata.mongowp.server.decoder.UpdateMessageDecoder;
import com.eightkdata.mongowp.server.encoder.ReplyMessageEncoder;
import com.eightkdata.mongowp.server.wp.DefaultRequestIdGenerator;
import com.eightkdata.mongowp.server.wp.NettyMongoServer;
import com.eightkdata.mongowp.server.wp.ReplyMessageObjectHandler;
import com.eightkdata.mongowp.server.wp.RequestIdGenerator;
import com.eightkdata.mongowp.server.wp.RequestMessageByteHandler;
import com.eightkdata.mongowp.server.wp.RequestMessageObjectHandler;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.ToroErrorHandler;
import com.torodb.mongodb.wp.TorodbSafeRequestProcessor;

/**
 *
 */
public class MongoDbWpModule extends PrivateModule {

  private final int port;

  public MongoDbWpModule(int port) {
    this.port = port;
  }

  @Override
  protected void configure() {
    bind(NettyMongoServer.class)
        .in(Singleton.class);
    expose(NettyMongoServer.class);

    bind(MongoServerConfig.class)
        .toInstance((MongoServerConfig) () -> port);
    expose(MongoServerConfig.class);

    bind(NettyStringReader.class)
        .to(PooledNettyStringReader.class)
        .in(Singleton.class);

    configureStringPool();

    bind(RequestIdGenerator.class)
        .to(DefaultRequestIdGenerator.class);

    bind(ErrorHandler.class)
        .to(ToroErrorHandler.class)
        .in(Singleton.class);

    bind(RequestMessageByteHandler.class)
        .in(Singleton.class);

    bindMessageDecoder();

    bind(DefaultNettyBsonLowLevelReader.class)
        .in(Singleton.class);
    bind(NettyBsonDocumentReader.class)
        .in(Singleton.class);
    bind(OffHeapNettyBsonLowLevelReader.class)
        .in(Singleton.class);

    bind(OffHeapValuesNettyBsonLowLevelReader.class)
        .to(OffHeapNettyBsonLowLevelReader.class);

    bind(RequestMessageObjectHandler.class)
        .in(Singleton.class);
    bind(ReplyMessageObjectHandler.class)
        .in(Singleton.class);
    bind(TorodbSafeRequestProcessor.class)
        .in(Singleton.class);

    bind(ReplyMessageEncoder.class)
        .in(Singleton.class);
    bind(MongodMetrics.class)
        .in(Singleton.class);
    bind(NettyBsonDocumentWriter.class)
        .in(Singleton.class);
  }

  private void bindMessageDecoder() {
    bind(MessageDecoderLocator.class)
        .in(Singleton.class);
    bind(DeleteMessageDecoder.class)
        .in(Singleton.class);
    bind(GetMoreMessageDecoder.class)
        .in(Singleton.class);
    bind(InsertMessageDecoder.class)
        .in(Singleton.class);
    bind(KillCursorsMessageDecoder.class)
        .in(Singleton.class);
    bind(QueryMessageDecoder.class)
        .in(Singleton.class);
    bind(UpdateMessageDecoder.class)
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
