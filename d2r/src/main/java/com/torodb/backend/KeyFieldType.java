package com.torodb.backend;

import javax.annotation.Nonnull;

import com.torodb.core.transaction.metainf.FieldType;

public class KeyFieldType {
        private final String key;
        private final FieldType fieldType;
        
        public KeyFieldType(@Nonnull String key, @Nonnull FieldType fieldType) {
            super();
            this.key = key;
            this.fieldType = fieldType;
        }
        
        public String getKey() {
            return key;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + fieldType.ordinal();
            result = prime * result + key.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            KeyFieldType other = (KeyFieldType) obj;
            if (fieldType != other.fieldType)
                return false;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return key + ':' + fieldType.name();
        }
    }
    
    