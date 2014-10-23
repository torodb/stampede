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

/**
 *
 * @param <Arg>
 */
public abstract class ValueDFW<Arg> implements ValueVisitor<Void, Arg>{
    
    protected void preDefaultValue(Value value, Arg arg) {
    }

    protected void postDefaultValue(Value value, Arg arg) {
    }

    protected void preInt(IntegerValue value, Arg arg) {
    }

    protected void postInt(IntegerValue value, Arg arg) {
    }

    @Override
    public Void visit(IntegerValue value, Arg arg) {
        preDefaultValue(value, arg);
        preInt(value, arg);

        postInt(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preLong(LongValue value, Arg arg) {
    }

    protected void postLong(LongValue value, Arg arg) {
    }

    @Override
    public Void visit(LongValue value, Arg arg) {
        preDefaultValue(value, arg);
        preLong(value, arg);

        postLong(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preString(StringValue value, Arg arg) {
    }

    protected void postString(StringValue value, Arg arg) {
    }

    @Override
    public Void visit(StringValue value, Arg arg) {
        preDefaultValue(value, arg);
        preString(value, arg);

        postString(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preDouble(DoubleValue value, Arg arg) {
    }

    protected void postDouble(DoubleValue value, Arg arg) {
    }

    @Override
    public Void visit(DoubleValue value, Arg arg) {
        preDefaultValue(value, arg);
        preDouble(value, arg);

        postDouble(value, arg);
        postDefaultValue(value, arg);

        return null;
    }

    protected void preBooleanValue(BooleanValue value, Arg arg) {
    }

    protected void postBooleanValue(BooleanValue value, Arg arg) {
    }
    
    @Override
    public Void visit(BooleanValue value, Arg arg) {
        preDefaultValue(value, arg);
        preBooleanValue(value, arg);
        
        postBooleanValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preNullValue(NullValue value, Arg arg) {
    }

    protected void postNullValue(NullValue value, Arg arg) {
    }

    @Override
    public Void visit(NullValue value, Arg arg) {
        preDefaultValue(value, arg);
        preNullValue(value, arg);
        
        postNullValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preArrayValue(ArrayValue value, Arg arg) {
    }

    protected void postArrayValue(ArrayValue value, Arg arg) {
    }

    @Override
    public Void visit(ArrayValue value, Arg arg) {
        preDefaultValue(value, arg);
        preArrayValue(value, arg);
        
        for (Value e : value) {
            e.accept(this, arg);
        }
        
        postArrayValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }
}
