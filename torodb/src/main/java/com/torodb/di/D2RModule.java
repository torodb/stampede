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
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.d2r.DefaultD2RTranslator;
import javax.inject.Singleton;

/**
 *
 */
public class D2RModule extends AbstractModule {
   
    @Override
    protected void configure() {
        bind(D2RTranslator.class).to(DefaultD2RTranslator.class).in(Singleton.class);
    }
}
