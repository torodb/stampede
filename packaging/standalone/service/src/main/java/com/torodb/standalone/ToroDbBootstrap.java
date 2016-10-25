
package com.torodb.standalone;

import java.time.Clock;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.standalone.config.model.Config;

/**
 *
 */
public class ToroDbBootstrap {

    private ToroDbBootstrap() {}

    public static Service createStandaloneService(Config config, Clock clock) {
        Injector bootstrapInjector = Guice.createInjector(new BootstrapModule(
                config, clock));
        ThreadFactory threadFactory = bootstrapInjector.getInstance(
                ThreadFactory.class);
        
        return new ToroDbService(threadFactory, bootstrapInjector);
    }

}
