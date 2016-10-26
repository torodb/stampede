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
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@ThreadSafe
public class Shutdowner implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(Shutdowner.class);

    @SuppressWarnings("rawtypes")
    private final List<WeakReference<ShutdownCallback>> closeables = new ArrayList<>();

    public synchronized void addCloseShutdownListener(AutoCloseable autoCloseable) {
        closeables.add(new WeakReference<>(new AutoCloseableShutdownCallback(autoCloseable)));
    }

    public synchronized void addStopShutdownListener(Service service) {
        closeables.add(new WeakReference<>(new ServiceShutdownCallback(service)));
    }

    public synchronized <R> void addShutdownListener(R resource, ShutdownListener<R> shutdownListener) {
        closeables.add(new WeakReference<>(new ShutdownListenerShutdownCallback<>(resource, shutdownListener)));
    }

    public synchronized void compact() {
        closeables.removeIf(ref -> ref.get() == null);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public synchronized void close() {
        Lists.reverse(closeables).forEach(ref -> {
            ShutdownCallback listener = ref.get();
            if (listener != null) {
                try {
                    listener.onShutdown();
                } catch (Throwable t) {
                    LOGGER.error("Error while trying to notify the a shutdown", t);
                }
            }
        });
    }

    public static interface ShutdownListener<E> {
        public void onShutdown(E e) throws Exception;
    }

    public static abstract class ShutdownCallback<E> {
        private final E resource;

        public ShutdownCallback(E resource) {
            this.resource = resource;
        }

        public E getResource() {
            return resource;
        }

        public abstract void onShutdown() throws Exception;
    }

    private static class ShutdownListenerShutdownCallback<E> extends ShutdownCallback<E> {
        private final ShutdownListener<E> listener;

        public ShutdownListenerShutdownCallback(E resource, ShutdownListener<E> listener) {
            super(resource);
            this.listener = listener;
        }

        @Override
        public void onShutdown() throws Exception {
            listener.onShutdown(getResource());
        }
    }

    private static class ServiceShutdownCallback extends ShutdownCallback<Service> {

        public ServiceShutdownCallback(Service resource) {
            super(resource);
        }
        
        @Override
        public void onShutdown() throws Exception {
            getResource().stopAsync();
            getResource().awaitTerminated();
        }
    }

    private static class AutoCloseableShutdownCallback extends ShutdownCallback<AutoCloseable> {

        public AutoCloseableShutdownCallback(AutoCloseable resource) {
            super(resource);
        }
        
        @Override
        public void onShutdown() throws Exception {
            getResource().close();
        }
    }

}
