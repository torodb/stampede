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
 * along with kvdocument-core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.kvdocument.values.utils;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.io.ByteProcessor;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.CharSource;
import java.io.*;
import java.nio.charset.Charset;

/**
 *
 */
/**
 * An object that wraps {@link ByteSource} but do not throw {@link IOException}.
 */
public class NonIOByteSource implements Serializable {

    private static final long serialVersionUID = 4441628071714697039L;

    private transient ByteSource delegate;

    /**
     *
     * @param delegate it shall not throw IOException for any of its methods.
     */
    public NonIOByteSource(ByteSource delegate) {
        this.delegate = delegate;
    }

    public ByteSource getDelegate() {
        return delegate;
    }

    public InputStream openStream() {
        try {
            return delegate.openStream();
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    public CharSource asCharSource(Charset charset) {
        return delegate.asCharSource(charset);
    }

    public InputStream openBufferedStream() {
        try {
            return delegate.openBufferedStream();
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    public ByteSource slice(long offset, long length) {
        return delegate.slice(offset, length);
    }

    public boolean isEmpty() {
        try {
            return delegate.isEmpty();
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    public long size() {
        try {
            return delegate.size();
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    public long copyTo(OutputStream output) {
        try {
            return delegate.copyTo(output);
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    public long copyTo(ByteSink sink) {
        try {
            return delegate.copyTo(sink);
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    public byte[] read() {
        try {
            return delegate.read();
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    public HashCode hash(HashFunction hashFunction) {
        try {
            return delegate.hash(hashFunction);
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    public boolean contentEquals(NonIOByteSource other) {
        try {
            return delegate.contentEquals(other.delegate);
        } catch (IOException ex) {
            throw new AssertionError("An illegal IOException was throw");
        }
    }

    private void writeObject(final java.io.ObjectOutputStream out) throws IOException {
        if (delegate instanceof Serializable) {
            out.writeBoolean(true);
            out.writeObject(delegate);
        }
        else {
            long sizeLong = delegate.size();
            if (sizeLong > Integer.MAX_VALUE) {
                throw new IOException("The byte source is too long to be serialized");
            }
            int sizeInt = (int) sizeLong;
            out.writeBoolean(false);
            out.writeInt(sizeInt);
            delegate.read(new ByteProcessorImpl(out));
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        if (in.readBoolean()) {
            delegate = (ByteSource) in.readObject();
        }
        else {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            int read = 0;
            int off = 0;
            int remaining = size;
            while (read != -1 && remaining > 0) {
                read = in.read(bytes, off, remaining);
                off += read;
                remaining -= read;
                assert off + remaining == size;
            }
            if (read == -1) {
                throw new IOException("The end of the stream was reach before it was expected");
            }
            if (remaining < 0) {
                throw new IOException("A byte array of lenght " + size + " was expected, but only " + off + " bytes were found");
            }
            delegate = ByteSource.wrap(bytes);
        }
    }

    private static class ByteProcessorImpl implements ByteProcessor<Void> {

        private final ObjectOutputStream out;

        public ByteProcessorImpl(ObjectOutputStream out) {
            this.out = out;
        }

        @Override
        public boolean processBytes(byte[] buf, int off, int len) throws IOException {
            out.write(buf, off, len);
            return true;
        }

        @Override
        public Void getResult() {
            return null;
        }
    }
}
