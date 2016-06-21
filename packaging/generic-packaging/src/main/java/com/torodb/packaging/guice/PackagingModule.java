
package com.torodb.packaging.guice;

import com.google.inject.AbstractModule;
import java.time.Clock;

/**
 *
 */
public class PackagingModule extends AbstractModule {

    private final Clock clock;

    public PackagingModule(Clock clock) {
        this.clock = clock;
    }

    @Override
    protected void configure() {
        bind(Clock.class)
                .toInstance(clock);
    }

}
