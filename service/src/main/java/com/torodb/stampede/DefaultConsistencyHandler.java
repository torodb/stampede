/*
 * ToroDB Stampede
 * Copyright © 2016 8Kdata Technology (www.8kdata.com)
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
package com.torodb.stampede;

import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.MetaInfoKey;
import com.torodb.core.retrier.Retrier;

import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class DefaultConsistencyHandler extends AbstractConsistencyHandler {

  private static final MetaInfoKey CONSISTENCY_KEY = () -> "repl.consistent";

  public DefaultConsistencyHandler(BackendService backendService, Retrier retrier,
      ThreadFactory threadFactory) {
    super(backendService, retrier, threadFactory);
  }

  @Override
  public MetaInfoKey getConsistencyKey() {
    return CONSISTENCY_KEY;
  }

}
