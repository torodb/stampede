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

package com.torodb.backend;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.metainf.FieldType;

/**
 *
 */
@Singleton
public abstract class AbstractIdentifierConstraints implements IdentifierConstraints {

    private static final char SEPARATOR = '_';
    private static final char ARRAY_DIMENSION_SEPARATOR = '$';
    
    private final ImmutableMap<FieldType, Character> fieldTypeIdentifiers;
    private final ImmutableMap<FieldType, String> scalarFieldTypeIdentifiers;
    private final ImmutableSet<String> restrictedSchemaNames;
    private final ImmutableSet<String> restrictedColumnNames;
    
    protected AbstractIdentifierConstraints(ImmutableSet<String> restrictedSchemaNames, ImmutableSet<String> restrictedColumnNames) {
        this.fieldTypeIdentifiers = Maps.immutableEnumMap(ImmutableMap.<FieldType, Character>builder()
            .put(FieldType.BINARY,               Character.valueOf('r')) // [r]aw
            .put(FieldType.BOOLEAN,              Character.valueOf('b')) // [b]inary
            .put(FieldType.DATE,                 Character.valueOf('c')) // [c]alendar
            .put(FieldType.DOUBLE,               Character.valueOf('d')) // [d]ouble
            .put(FieldType.INSTANT,              Character.valueOf('g')) // [G]eorge Gamow or Admiral [G]race Hopper that were the earliest users of the term nanosecond
            .put(FieldType.INTEGER,              Character.valueOf('i')) // [i]nteger
            .put(FieldType.LONG,                 Character.valueOf('l')) // [l]ong
            .put(FieldType.MONGO_OBJECT_ID,      Character.valueOf('x'))
            .put(FieldType.MONGO_TIME_STAMP,     Character.valueOf('y'))
            .put(FieldType.NULL,                 Character.valueOf('n')) // [n]ull
            .put(FieldType.STRING,               Character.valueOf('s')) // [s]tring
            .put(FieldType.TIME,                 Character.valueOf('t')) // [t]ime
            .put(FieldType.CHILD,                Character.valueOf('e')) // [e]lement
            .build());
        
        ImmutableMap.Builder<FieldType, String> scalarFieldTypeIdentifiersBuilder = 
                ImmutableMap.<FieldType, String>builder();
        Set<Character> fieldTypeIdentifierSet = new HashSet<>();
        for (FieldType fieldType : FieldType.values()) {
            if (!this.fieldTypeIdentifiers.containsKey(fieldType)) {
                throw new SystemException("FieldType " + fieldType + " has not been mapped to an identifier.");
            }
            
            char identifier = this.fieldTypeIdentifiers.get(fieldType);
            
            if ((identifier < 'a' || identifier > 'z') &&
                    (identifier < '0' || identifier > '9')) {
                throw new SystemException("FieldType " + fieldType + " has an unallowed identifier " 
                        + identifier);
            }
            
            if (fieldTypeIdentifierSet.contains(identifier)) {
                throw new SystemException("FieldType " + fieldType + " identifier " 
                        + identifier + " was used by another FieldType.");
            }
            
            fieldTypeIdentifierSet.add(identifier);
            
            scalarFieldTypeIdentifiersBuilder.put(fieldType, DocPartTableFields.SCALAR.fieldName + SEPARATOR + identifier);
        }
        
        this.scalarFieldTypeIdentifiers = Maps.immutableEnumMap(scalarFieldTypeIdentifiersBuilder.build());
        
        this.restrictedSchemaNames = ImmutableSet.<String>builder()
                .add(TorodbSchema.IDENTIFIER)
                .addAll(restrictedSchemaNames)
                .build();
        
        this.restrictedColumnNames = ImmutableSet.<String>builder()
                .add(DocPartTableFields.DID.fieldName)
                .add(DocPartTableFields.RID.fieldName)
                .add(DocPartTableFields.PID.fieldName)
                .add(DocPartTableFields.SEQ.fieldName)
                .addAll(scalarFieldTypeIdentifiers.values())
                .addAll(restrictedColumnNames)
                .build();
    }
    
    @Override
    public char getSeparator() {
        return SEPARATOR;
    }

    @Override
    public char getArrayDimensionSeparator() {
        return ARRAY_DIMENSION_SEPARATOR;
    }

    @Override
    public boolean isAllowedSchemaIdentifier(@Nonnull String schemaName) {
        return !restrictedSchemaNames.contains(schemaName);
    }
    
    @Override
    public boolean isAllowedTableIdentifier(@Nonnull String columnName) {
        return true;
    }
    
    @Override
    public boolean isAllowedColumnIdentifier(@Nonnull String columnName) {
        return !restrictedColumnNames.contains(columnName);
    }

    @Override
    public boolean isSameIdentifier(@Nonnull String leftIdentifier, @Nonnull String rightIdentifier) {
        return leftIdentifier.equals(rightIdentifier); //leftIdentifier.toLowerCase(Locale.US).equals(rightIdentifier.toLowerCase(Locale.US));
    }

    @Override
    public char getFieldTypeIdentifier(FieldType fieldType) {
        return fieldTypeIdentifiers.get(fieldType);
    }

    @Override
    public String getScalarIdentifier(FieldType fieldType) {
        return scalarFieldTypeIdentifiers.get(fieldType);
    }
}
