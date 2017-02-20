package com.torodb.d2r;

import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.types.*;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.heap.ListKvArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestedValuesKvTypeVisitor implements KvTypeVisitor<List<KvValue>, Void> {
    @Override
    public List<KvValue> visit(ArrayType type, Void arg) {
        return Arrays.asList(new ListKvArray(Arrays.asList(new KvValue[] {KvBoolean.FALSE,KvInteger.of(123)})));
    }

    @Override
    public List<KvValue> visit(BooleanType type, Void arg) {
        return Arrays.asList( new KvBoolean[]{
                KvBoolean.FALSE,
                KvBoolean.TRUE
        });
    }

    @Override
    public List<KvValue> visit(DoubleType type, Void arg) {
        return Arrays.asList(new KvDouble[]{
                KvDouble.of(2.55)
        });
    }

    @Override
    public List<KvValue> visit(IntegerType type, Void arg) {
        return Arrays.asList(new KvInteger[]{
                KvInteger.of(123)
        });
    }

    @Override
    public List<KvValue> visit(LongType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(NullType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(DocumentType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(StringType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(GenericType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(MongoObjectIdType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(InstantType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(DateType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(TimeType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(BinaryType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(NonExistentType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(MongoTimestampType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(Decimal128Type type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(JavascriptType type, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(JavascriptWithScopeType value, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(MinKeyType value, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(MaxKeyType value, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(UndefinedType value, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(MongoRegexType value, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(MongoDbPointerType value, Void arg) {
        return new ArrayList<>();
    }

    @Override
    public List<KvValue> visit(DeprecatedType value, Void arg) {
        return new ArrayList<>();
    }

    public <T extends KvType> List<KvValue> visit(T value, Void arg) {
        return new ArrayList<>();
    }
}
