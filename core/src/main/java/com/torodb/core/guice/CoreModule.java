/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.core.Retrier;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.RetrierImpl;
import com.torodb.core.impl.TableRefFactoryImpl;

/**
 *
 */
public class CoreModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TableRefFactory.class)
                .to(TableRefFactoryImpl.class)
                .asEagerSingleton();

        bind(Retrier.class)
                .to(RetrierImpl.class)
                .in(Singleton.class);

    }

}
