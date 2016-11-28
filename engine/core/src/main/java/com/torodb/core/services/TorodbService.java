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

package com.torodb.core.services;

import com.google.common.util.concurrent.Service;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
public interface TorodbService extends Service {

  public default CompletableFuture<?> stop() {
    this.stopAsync();
    return CompletableFuture.runAsync(this::awaitRunning);
  }

  public default CompletableFuture<?> start() {
    this.startAsync();
    return CompletableFuture.runAsync(this::awaitTerminated);
  }

}
