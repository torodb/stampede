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

package com.torodb.kvdocument.values;

import com.google.common.collect.Maps;
import com.torodb.kvdocument.types.ObjectType;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.threeten.bp.LocalDate;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.LocalTime;

/**
 *
 */
public class ObjectValue implements DocValue {

    private final Map<String, DocValue> values;

    public ObjectValue(Map<String, DocValue> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    @Nonnull
    public DocValue get(String key) {
        if (!values.containsKey(key)) {
            throw new IllegalArgumentException(key + " is not a key of this document");
        }
        assert values.get(key) != null;
        return values.get(key);
    }

    @Override
    public Map<String, DocValue> getValue() {
        return values;
    }

    @Override
    public ObjectType getType() {
        return ObjectType.INSTANCE;
    }

    public Set<String> keySet() {
        return values.keySet();
    }

    public boolean contains(String key) {
        return values.containsKey(key);
    }

    public Collection<DocValue> values() {
        return values.values();
    }

    public Set<Map.Entry<String, DocValue>> getAttributes() {
        return values.entrySet();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (Map.Entry<String, DocValue> entry : values.entrySet()) {
            sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append(", ");
        }
        if (!values.isEmpty()) {
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (this.values != null ? this.values.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ObjectValue other = (ObjectValue) obj;
        if (
                this.values != other.values 
                && (this.values == null || !this.values.equals(other.values))) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(DocValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    public static class Builder {
        private Map<String, DocValue> values = Maps.newHashMap();
        private final Map<String, ArrayValue.Builder> subArrayBuilders = 
                Maps.newHashMap();
        private final Map<String, ObjectValue.Builder> subObjectBuilders = 
                Maps.newHashMap();
        private boolean built = false;
        
        public void clear() {
            if (built) {
                built = false;
                values = Maps.newHashMap();
            }
            else {
                values.clear();
            }
            subArrayBuilders.clear();
            subObjectBuilders.clear();
        }
        
        public static Builder from(ObjectValue original) {
            Builder result = new Builder();
            result.copy(original);
            
            return result;
        }
        
        public boolean contains(String key) {
            return isValue(key)
                    || isArrayBuilder(key)
                    || isObjectBuilder(key);
        }
        
        public boolean isValue(String key) {
            return values.containsKey(key);
        }
        
        @Nonnull
        public DocValue getValue(String key) {
            DocValue result = values.get(key);
            if (result == null) {
                throw new IllegalArgumentException(
                        "There is no value associated to '"+key+"' key");
            }
            return result;
        }
        
        public boolean isArrayBuilder(String key) {
            return subArrayBuilders.containsKey(key);
        }
        
        @Nonnull
        public ArrayValue.Builder getArrayBuilder(String key) {
            ArrayValue.Builder result = subArrayBuilders.get(key);
            if (result == null) {
                throw new IllegalArgumentException(
                        "There is no array builder associated to '"+key+"' key");
            }
            return result;
        }
        
        public boolean isObjectBuilder(String key) {
            return subObjectBuilders.containsKey(key);
        }
        
        @Nonnull
        public ObjectValue.Builder getObjectBuilder(String key) {
            ObjectValue.Builder result = subObjectBuilders.get(key);
            if (result == null) {
                throw new IllegalArgumentException(
                        "There is no object builder associated to '"+key+"' key");
            }
            return result;
        }
        
        public Builder putValue(String key, DocValue value) {
            checkNewBuild();
            
            if (value instanceof ObjectValue) {
                newObject(key).copy((ObjectValue) value);
            }
            else {
                if (value instanceof ArrayValue) {
                    newArray(key).copy((ArrayValue) value);
                }
                else {
                    values.put(key, value);
                    subArrayBuilders.remove(key);
                    subObjectBuilders.remove(key);
                }
            }
            
            return this;
        }
        
        public Builder putValue(String key, boolean value) {
            return putValue(key, value ? BooleanValue.TRUE : BooleanValue.FALSE);
        }
        
        public Builder putValue(String key, LocalDateTime value) {
            return putValue(key, new DateTimeValue(value));
        }
        
        public Builder putValue(String key, LocalDate value) {
            return putValue(key, new DateValue(value));
        }
        
        public Builder putValue(String key, double value) {
            return putValue(key, new DoubleValue(value));
        }
        
        public Builder putValue(String key, int value) {
            return putValue(key, new IntegerValue(value));
        }
        
        public Builder putValue(String key, long value) {
            return putValue(key, new LongValue(value));
        }
        
        public Builder putNullValue(String key) {
            return putValue(key, NullValue.INSTANCE);
        }
        
        public Builder putValue(String key, String value) {
            return putValue(key, new StringValue(value));
        }
        
        public Builder putValue(String key, LocalTime value) {
            return putValue(key, new TimeValue(value));
        }
        
        public Builder putValue(String key, ObjectValue.Builder value) {
            return putValue(key, value.build());
        }
        
        public Builder putValue(String key, ArrayValue.Builder value) {
            return putValue(key, value.build());
        }
        
        public ArrayValue.Builder newArray(String key) {
            checkNewBuild();
            
            ArrayValue.Builder result = new ArrayValue.Builder();
            
            values.remove(key);
            subArrayBuilders.put(key, result);
            subObjectBuilders.remove(key);
            
            return result;
        }
        
        public ObjectValue.Builder newObject(String key) {
            checkNewBuild();
            
            ObjectValue.Builder result = new ObjectValue.Builder();
            
            values.remove(key);
            subArrayBuilders.remove(key);
            subObjectBuilders.put(key, result);
            
            return result;
        }
        
        public boolean unset(String key) {
            boolean result = false;
            result |= values.remove(key) != null;
            result |= subArrayBuilders.remove(key) != null;
            result |= subObjectBuilders.remove(key) != null;
            
            return result;
        }
        
        public ObjectValue build() {
            built = true;
            
            for (Map.Entry<String, ObjectValue.Builder> objectBuilder 
                    : subObjectBuilders.entrySet()) {
                
                DocValue oldValue 
                        = values.put(
                                objectBuilder.getKey(), 
                                objectBuilder.getValue().build()
                        );
                
                assert oldValue == null;
            }
            for (Map.Entry<String, ArrayValue.Builder> arrayBuilder 
                    : subArrayBuilders.entrySet()) {
                
                DocValue oldValue 
                        = values.put(
                                arrayBuilder.getKey(), 
                                arrayBuilder.getValue().build()
                        );
                
                assert oldValue == null;
            }
            subObjectBuilders.clear();
            subArrayBuilders.clear();

            return new ObjectValue(values);
        }
        
        void copy(ObjectValue original) {
            values.clear();
            subArrayBuilders.clear();
            subObjectBuilders.clear();
            
            for (Map.Entry<String, DocValue> attribute 
                    : original.getAttributes()) {
                
                DocValue value = attribute.getValue();
                if (value instanceof ArrayValue) {
                    ArrayValue.Builder childBuilder = newArray(attribute.getKey());
                    childBuilder.copy((ArrayValue) value);
                }
                else if (value instanceof ObjectValue) {
                    Builder childBuilder = newObject(attribute.getKey());
                    childBuilder.copy((ObjectValue) value);
                }
                else {
                    putValue(attribute.getKey(), value);
                }
            }
        }
        
        private void checkNewBuild() {
            if (built) {
                values = Maps.newHashMap();
                built = false;
            }
        }
    }
}
