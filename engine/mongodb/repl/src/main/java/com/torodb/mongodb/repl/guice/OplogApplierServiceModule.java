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

package com.torodb.mongodb.repl.guice;

import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.mongodb.repl.DefaultOplogApplierService;
import com.torodb.mongodb.repl.OplogApplierService;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.OplogReaderProvider;
import com.torodb.mongodb.repl.ReplMetrics;
import com.torodb.mongodb.repl.SyncSourceProvider;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier;
import com.torodb.mongodb.repl.oplogreplier.fetcher.ContinuousOplogFetcher;


public class OplogApplierServiceModule extends PrivateModule {
  
  @Override
  protected void configure() {
    expose(OplogApplierService.OplogApplierServiceFactory.class);

    requireBinding(OplogManager.class);
    requireBinding(SyncSourceProvider.class);
    requireBinding(ReplMetrics.class);
    requireBinding(OplogReaderProvider.class);
    requireBinding(OplogApplier.class);

    install(new FactoryModuleBuilder()
        //To use the old applier that emulates MongoDB
        //                .implement(OplogApplierService.class, SequentialOplogApplierService.class)

        //To use the applier service that delegates on a OplogApplier
        .implement(OplogApplierService.class, DefaultOplogApplierService.class)
        .build(OplogApplierService.OplogApplierServiceFactory.class)
    );
    

    install(new FactoryModuleBuilder()
        .implement(ContinuousOplogFetcher.class, ContinuousOplogFetcher.class)
        .build(ContinuousOplogFetcher.ContinuousOplogFetcherFactory.class)
    );
  }

}
