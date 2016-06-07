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

package com.torodb.torod.tools.sequencer;



import java.util.EnumMap;
import java.util.EnumSet;
import java.util.logging.Logger;

/**
 *
 */
public class Sequencer<E extends Enum<E>> {

    private final EnumSet<E> sentMessages;
    private final EnumMap<E, java.lang.Thread> reservedMessages;
    private static final Logger LOG = Logger.getLogger(Sequencer.class.getName());

    public Sequencer(Class<E> messagesEnum) {
        sentMessages = EnumSet.noneOf(messagesEnum);
        reservedMessages = new EnumMap<>(messagesEnum);
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
}
