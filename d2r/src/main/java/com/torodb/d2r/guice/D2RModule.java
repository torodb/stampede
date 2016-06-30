
package com.torodb.d2r.guice;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;

/**
 *
 */
public class D2RModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new FactoryModuleBuilder()
                .implement(D2RTranslator.class, D2RTranslatorStack.class)
                .build(D2RTranslatorFactory.class)
        );

        bind(IdentifierFactory.class)
                .to(IdentifierFactoryImpl.class)
                .asEagerSingleton();

//        bind(new TypeLiteral<R2DTranslator<ResultSet>>() {})
//                .to(R2DBackedTranslator.class);
    }

}
