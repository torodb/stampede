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

package com.torodb.common.util;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
public class ListeningFutureToCompletableFuture {

  private ListeningFutureToCompletableFuture() {
  }

  public static <T> CompletableFuture<T> toCompletableFuture(
      final ListenableFuture<T> listenableFuture) {
    //create an instance of CompletableFuture
    CompletableFuture<T> completable = new CompletableFuture<T>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        // propagate cancel to the listenable future
        boolean result = listenableFuture.cancel(mayInterruptIfRunning);
        super.cancel(mayInterruptIfRunning);
        return result;
      }
    };

    // add callback
    Futures.addCallback(listenableFuture, new FutureCallback<T>() {
      @Override
      @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
      public void onSuccess(T result) {
        completable.complete(result);
      }

      @Override
      public void onFailure(Throwable t) {
        completable.completeExceptionally(t);
      }
    });
    return completable;
  }
}
