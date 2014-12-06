
package com.torodb.di;

import com.google.inject.AbstractModule;
import com.toro.torod.cursors.DefaultInnerCursorManager;
import com.torodb.torod.core.cursors.InnerCursorManager;
import javax.inject.Singleton;

/**
 *
 */
public class InnerCursorManagerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(InnerCursorManager.class).to(DefaultInnerCursorManager.class).in(Singleton.class);
    }
    
    
}