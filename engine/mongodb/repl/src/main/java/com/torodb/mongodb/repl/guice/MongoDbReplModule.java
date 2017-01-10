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

import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.mongodb.repl.RecoveryService;
import com.torodb.mongodb.repl.ReplCoordinator;
import com.torodb.mongodb.repl.ReplCoordinatorStateMachine;
import com.torodb.mongodb.repl.commands.ReplCommandsGuiceModule;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.cloner.CommitHeuristic;

import javax.inject.Singleton;

public class MongoDbReplModule extends PrivateModule {

  @Override
  protected void configure() {
    expose(ReplCoordinator.class);
    expose(Key.get(DbCloner.class, MongoDbRepl.class));

    bind(ReplCoordinator.class)
        .in(Singleton.class);
    bind(ReplCoordinatorStateMachine.class)
        .in(Singleton.class);

    install(new FactoryModuleBuilder()
        .implement(RecoveryService.class, RecoveryService.class)
        .build(RecoveryService.RecoveryServiceFactory.class)
    );

    bind(DbCloner.class)
        .annotatedWith(MongoDbRepl.class)
        .toProvider(AkkaDbClonerProvider.class)
        .in(Singleton.class);

    bind(CommitHeuristic.class)
        .to(DefaultCommitHeuristic.class)
        .in(Singleton.class);

    bind(Integer.class)
        .annotatedWith(DocsPerTransaction.class)
        .toInstance(1000);

    install(new ReplCommandsGuiceModule());
  }

  public static class DefaultCommitHeuristic implements CommitHeuristic {

    @Override
    public void notifyDocumentInsertionCommit(int docBatchSize, long millisSpent) {
    }

    @Override
    public int getDocumentsPerCommit() {
      return 1000;
    }

    @Override
    public boolean shouldCommitAfterIndex() {
      return false;
    }
  }
}
