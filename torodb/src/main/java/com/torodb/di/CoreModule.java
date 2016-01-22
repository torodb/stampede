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
 * along with torodb. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.di;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.util.Providers;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.subdocument.BufferedSubDocTypeBuilderProvider;
import com.torodb.torod.core.subdocument.SimpleSubDocTypeBuilderProvider;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocType.Builder;
import com.torodb.torod.d2r.DefaultD2RTranslator;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
public class CoreModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SubDocType.Builder.class).
                toProvider(ProviderAdaptor.class);
    }

    @Singleton
    public static class ProviderAdaptor implements Provider<SubDocType.Builder> {
        private final javax.inject.Provider<SubDocType.Builder> delegate;

        @Inject
        public ProviderAdaptor(BufferedSubDocTypeBuilderProvider delegate) {
            this.delegate = delegate;
        }

        @Override
        public Builder get() {
            return delegate.get();
        }
    }
}
