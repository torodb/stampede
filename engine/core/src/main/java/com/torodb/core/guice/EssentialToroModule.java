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

package com.torodb.core.guice;

import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.binder.LinkedBindingBuilder;

/**
 * The {@link PrivateModule} extended by that essential ToroDB modules.
 *
 * <p/>It contains some syntacthic sugar methods designed to make it easy to create essential
 * bindings.
 */
public abstract class EssentialToroModule extends PrivateModule {

  protected final <T> void exposeEssential(Class<T> clazz) {
    expose(Key.get(clazz, Essential.class));
  }

  protected final <T> LinkedBindingBuilder<T> bindEssential(Class<T> clazz) {
    return bind(clazz)
        .annotatedWith(Essential.class);
  }

}
