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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.EnumSet;

/**
 * This class is used to sequencialize several threads.
 *
 * It is mainly used on concurrency scenarios when a test want to force an specific sequence of
 * events.
 *
 * Any sequencer is associated with an enum class, whose elements are called <em>messages</em>.
 * Threads can wait until a message is sent calling the method {@link #waitFor(java.lang.Enum) } or
 * can send messages with {@link #notify(java.lang.Enum) }.
 */
public class Sequencer<E extends Enum<E>> {

  private final EnumSet<E> sentMessages;
  private final Multimap<E, Thread> reservedMessages;

  public Sequencer(Class<E> messageClass) {
    sentMessages = EnumSet.noneOf(messageClass);
    reservedMessages = HashMultimap.create();
  }

  public void waitFor(E message) {
    synchronized (message) {
      reservedMessages.put(message, Thread.currentThread());
      while (!sentMessages.contains(message)) {
        try {
          message.wait();
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public void notify(E message) {
    synchronized (message) {
      sentMessages.add(message);
      message.notifyAll();
    }
  }

  @SuppressWarnings("unchecked")
  public void notify(E... message) {
    for (E e : message) {
      notify(e);
    }
  }
}
