
package com.torodb.di;

import com.google.inject.AbstractModule;
import com.toro.torod.cursors.DefaultInnerCursorManager;
import com.torodb.torod.core.cursors.InnerCursorManager;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.db.metaInf.DefaultDbMetaInformationCache;
import com.torodb.torod.db.metaInf.DefaultTableMetaInfoFactory;
import com.torodb.torod.db.metaInf.ReservedIdHeuristic;
import com.torodb.torod.db.metaInf.ReservedIdInfoFactory;
import com.torodb.torod.db.metaInf.idHeuristic.PoolReserveIdHeuristic;
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