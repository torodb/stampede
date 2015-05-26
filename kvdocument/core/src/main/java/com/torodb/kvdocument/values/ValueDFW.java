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

/**
 *
 * @param <Arg>    The auxiliary argument of the pre and post methods. If it is not needed, {@link java.lang.Void} can
 *                 be used
 */
public class ValueDFW<Arg> implements DocValueVisitor<Void, Arg> {

    protected void preDefaultValue(DocValue value, Arg arg) {
    }

    protected void postDefaultValue(DocValue value, Arg arg) {
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

    protected void preObjectValue(ObjectValue value, Arg arg) {
    }

    protected void postObjectValue(ObjectValue value, Arg arg) {
    }

    @Override
    public Void visit(ObjectValue value, Arg arg) {
        preDefaultValue(value, arg);
        preObjectValue(value, arg);

        for (DocValue childValue : value.values()) {
            childValue.accept(this, arg);
        }

        postObjectValue(value, arg);
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
        
        for (DocValue element : value) {
            element.accept(this, arg);
        }
        
        postArrayValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preTwelveBytesValue(TwelveBytesValue value, Arg arg) {
    }

    protected void postTwelveBytesValue(TwelveBytesValue value, Arg arg) {
    }

    @Override
    public Void visit(TwelveBytesValue value, Arg arg) {
        
        preDefaultValue(value, arg);
        preTwelveBytesValue(value, arg);
        
        postTwelveBytesValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preDateTimeValue(DateTimeValue value, Arg arg) {
    }

    protected void postDateTimeValue(DateTimeValue value, Arg arg) {
    }

    @Override
    public Void visit(DateTimeValue value, Arg arg) {
        
        preDefaultValue(value, arg);
        preDateTimeValue(value, arg);
        
        postDateTimeValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preDateValue(DateValue value, Arg arg) {
    }

    protected void postDateValue(DateValue value, Arg arg) {
    }

    @Override
    public Void visit(DateValue value, Arg arg) {
        
        preDefaultValue(value, arg);
        preDateValue(value, arg);
        
        postDateValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void preTimeValue(TimeValue value, Arg arg) {
    }

    protected void postTimeValue(TimeValue value, Arg arg) {
    }

    @Override
    public Void visit(TimeValue value, Arg arg) {
        
        preDefaultValue(value, arg);
        preTimeValue(value, arg);
        
        postTimeValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }

    protected void prePatternValue(PatternValue value, Arg arg) {
    }
    
    protected void postPatternValue(PatternValue value, Arg arg) {
    }
    
    @Override
    public Void visit(PatternValue value, Arg arg) {
        preDefaultValue(value, arg);
        prePatternValue(value, arg);
        
        postPatternValue(value, arg);
        postDefaultValue(value, arg);
        
        return null;
    }
    
    
}
