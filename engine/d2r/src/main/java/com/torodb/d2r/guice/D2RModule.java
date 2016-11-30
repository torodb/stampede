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

package com.torodb.d2r.guice;

import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.R2DTranslatorImpl;

/**
 *
 */
public class D2RModule extends PrivateModule {

  @Override
  protected void configure() {
    install(new FactoryModuleBuilder()
        .implement(D2RTranslator.class, D2RTranslatorStack.class)
        .build(D2RTranslatorFactory.class)
    );
    expose(D2RTranslatorFactory.class);

    bind(R2DTranslator.class)
        .to(R2DTranslatorImpl.class);
    expose(R2DTranslator.class);
  }

}
