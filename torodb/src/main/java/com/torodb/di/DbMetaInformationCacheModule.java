/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */


package com.torodb.di;

import com.google.inject.AbstractModule;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.db.backends.metaInf.DefaultDbMetaInformationCache;
import com.torodb.torod.db.backends.metaInf.DefaultTableMetaInfoFactory;
import com.torodb.torod.db.backends.metaInf.ReservedIdHeuristic;
import com.torodb.torod.db.backends.metaInf.ReservedIdInfoFactory;
import com.torodb.torod.db.backends.metaInf.idHeuristic.PoolReserveIdHeuristic;
import javax.inject.Singleton;

/**
 *
 */
public class DbMetaInformationCacheModule extends AbstractModule {


    public DbMetaInformationCacheModule() {
    }
    
    @Override
    protected void configure() {
        bind(DbMetaInformationCache.class).to(DefaultDbMetaInformationCache.class).in(Singleton.class);
//        bind(ReservedIdHeuristic.class).to(LazyReserveIdHeuristic.class).in(Singleton.class);
        bind(ReservedIdHeuristic.class).toInstance(new PoolReserveIdHeuristic(50000, 0.75));
        bind(ReservedIdInfoFactory.class).to(DefaultTableMetaInfoFactory.class).in(Singleton.class);
    }
    
    
}
