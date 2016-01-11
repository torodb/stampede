
package com.torodb.torod.core.connection;

/**
 *
 */
public final class TransactionMetainfo {
    public static final TransactionMetainfo READ_ONLY = new TransactionMetainfo(true);
    public static final TransactionMetainfo NOT_READ_ONLY = new TransactionMetainfo(false);
    private final boolean readOnly;

    private TransactionMetainfo(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public static class Builder {
        boolean readOnly = false;

        public Builder() {}

        public Builder(TransactionMetainfo other) {
            this.readOnly = other.readOnly;
        }

        public Builder setReadOnly(boolean readOnly) {
            this.readOnly = readOnly;
            return this;
        }

        public TransactionMetainfo build() {
            return new TransactionMetainfo(readOnly);
        }
    }

}
