
package com.torodb.mongodb.guice;

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
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.mongodb.commands.TorodbSafeRequestProcessor;
import com.torodb.mongodb.core.ToroErrorHandler;

/**
 *
 */
public class MongoLayerModule extends AbstractModule {

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
    RequestProcessor createRequestProcessorAdaptor(TorodbSafeRequestProcessor tsrp, ErrorHandler errorHandler) {
        return new RequestProcessorAdaptor<>(tsrp, errorHandler);
    }

}
