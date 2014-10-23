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


package com.torodb.torod.core.subdocument.values;

import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.kvdocument.values.DocValue;

/**
 *
 */
public class ValueFactory {

    private ValueFactory() {
    }

    public static Value<? extends Number> fromNumber(Number n) {
        if (n instanceof Integer) {
            return new IntegerValue((Integer) n);
        }
        if (n instanceof Double) {
            return new DoubleValue((Double) n);
        }
        if (n instanceof Long) {
            return new LongValue((Long) n);
        }
        throw new ToroImplementationException(n + " is not a valid basic type numeric number");
    }
    
    public static Value<?> fromDocValue(DocValue docValue) {
        if (docValue instanceof com.torodb.kvdocument.values.ArrayValue) {
            ArrayValue.Builder builder = new ArrayValue.Builder();
            for (DocValue child : ((com.torodb.kvdocument.values.ArrayValue) docValue)) {
                builder.add(fromDocValue(child));
            }
            return builder.build();
        }
        
        if (docValue instanceof com.torodb.kvdocument.values.BooleanValue) {
            Boolean wrappedValue = ((com.torodb.kvdocument.values.BooleanValue) docValue).getValue();
            
            return BooleanValue.from(wrappedValue);
        }
        
        if (docValue instanceof com.torodb.kvdocument.values.DoubleValue) {
            Double wrappedValue = ((com.torodb.kvdocument.values.DoubleValue) docValue).getValue();
            
            return new DoubleValue(wrappedValue);
        }
        
        if (docValue instanceof com.torodb.kvdocument.values.IntegerValue) {
            Integer wrappedValue = ((com.torodb.kvdocument.values.IntegerValue) docValue).getValue();
            
            return new IntegerValue(wrappedValue);
        }
        
        if (docValue instanceof com.torodb.kvdocument.values.LongValue) {
            Long wrappedValue = ((com.torodb.kvdocument.values.LongValue) docValue).getValue();
            
            return new LongValue(wrappedValue);
        }
        
        if (docValue instanceof com.torodb.kvdocument.values.NullValue) {
            return NullValue.INSTANCE;
        }
        
        if (docValue instanceof com.torodb.kvdocument.values.StringValue) {
            String wrappedValue = ((com.torodb.kvdocument.values.StringValue) docValue).getValue();
            
            return new StringValue(wrappedValue);
        }
        
        throw new ToroImplementationException("DocValue "+docValue + " is not translatable to basic values");
    }
 }
