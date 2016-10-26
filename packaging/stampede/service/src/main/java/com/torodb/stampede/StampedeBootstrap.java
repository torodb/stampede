
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
        Injector bootstrapInjector = Guice.createInjector(new BootstrapModule(
                config, clock));
        ThreadFactory threadFactory = bootstrapInjector.getInstance(
                ThreadFactory.class);
        
        return new StampedeService(threadFactory, bootstrapInjector);
    }

}
