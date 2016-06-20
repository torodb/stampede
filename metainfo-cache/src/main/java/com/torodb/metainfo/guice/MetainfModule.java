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
 * along with metainfo-cache. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.metainfo.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

/**
 *
 */
public class MetainfModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(MetainfoRepository.class)
                .to(MvccMetainfoRepository.class)
                .in(Singleton.class);
    }

}
