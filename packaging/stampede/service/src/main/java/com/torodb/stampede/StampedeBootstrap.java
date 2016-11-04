
package com.torodb.stampede;

import java.time.Clock;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.stampede.config.model.Config;

/**
 *
 */
public class StampedeBootstrap {

    private StampedeBootstrap() {}

    public static Service createStampedeService(Config config, Clock clock) {
        return createStampedeService(new BootstrapModule(
                config, clock));
    }

    public static Service createStampedeService(BootstrapModule bootstrapModule) {
        Injector bootstrapInjector = Guice.createInjector(bootstrapModule);
        ThreadFactory threadFactory = bootstrapInjector.getInstance(
                ThreadFactory.class);
        
        return new StampedeService(threadFactory, bootstrapInjector);
    }

}
