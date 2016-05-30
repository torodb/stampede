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

package com.torodb.common.util;



import java.util.EnumMap;
import java.util.EnumSet;

/**
 * This class is used to sequencialize several threads.
 *
 * It is mainly used on concurrency scenarios when a test want to force an specific sequence of events.
 *
 * Any sequencer is associated with an enum class, whose elements are called <em>messages</em>.
 * Threads can wait until a message is sent calling the method {@link #waitFor(java.lang.Enum) } or
 * can send messages with {@link #notify(java.lang.Enum) }.
 */
public class Sequencer<E extends Enum<E>> {

    private final Class<E> messagesClass;
    private final EnumSet<E> sentMessages;
    private final EnumMap<E, Thread> reservedMessages;

    public Sequencer(Class<E> messageClass) {
        this.messagesClass = messageClass;
        sentMessages = EnumSet.noneOf(messageClass);
        reservedMessages = new EnumMap<>(messageClass);
    }
    
    public void waitFor(E message) {
        synchronized (message) {
            assert !reservedMessages.containsKey(message) : 
                    "Thread '" + reservedMessages.get(message) + "' already reserved this message";
            reservedMessages.put(message, java.lang.Thread.currentThread());
            while (!sentMessages.contains(message)) {
                try {
                    message.wait();
                } catch (InterruptedException ex) {
                    throw new InterruptedExceptionRuntimeException(ex);
                }
            }
        }
    }
    
    public void notify(E message) {
        synchronized (message) {
            assert !sentMessages.contains(message) : "Message '"+message+"' has already been sent";
            sentMessages.add(message);
            message.notifyAll();
        }
    }

    public void notify(E... message) {
        for (E e : message) {
            notify(e);
        }
    }

    public static class InterruptedExceptionRuntimeException extends RuntimeException {

        private static final long serialVersionUID = 1L;
        private final InterruptedException ex;

        public InterruptedExceptionRuntimeException(InterruptedException ex) {
            super(ex);
            this.ex = ex;
        }

        public InterruptedExceptionRuntimeException(InterruptedException ex, String message) {
            super(message, ex);
            this.ex = ex;
        }

    }
}
