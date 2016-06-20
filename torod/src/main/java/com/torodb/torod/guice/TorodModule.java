
package com.torodb.torod.guice;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.torod.pipeline.akka.AkkaInsertSubscriberFactory;

/**
 *
 */
public class TorodModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(InsertPipelineFactory.class)
                .to(AkkaInsertSubscriberFactory.class)
                .in(Singleton.class);

        ActorSystem actorSystem = ActorSystem.create("torod-layer");

        bind(ActorSystem.class)
                .toInstance(actorSystem);

        bind(Materializer.class)
                .toInstance(ActorMaterializer.create(actorSystem));
    }

}
