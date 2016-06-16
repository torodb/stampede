package com.torodb.d2r;

import java.text.Normalizer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.torodb.core.TableRef;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaSnapshot;

public class IdentifierFactoryImpl implements IdentifierFactory {

    private static final int MAX_GENERATION_TIME = 10;
    private static final char SEPARATOR = '_';
    private static final String SEPARATOR_STRING = String.valueOf('_');
    private static final char[] FIELD_TYPE_IDENTIFIERS = new char[FieldType.values().length];
    static {
        FIELD_TYPE_IDENTIFIERS[FieldType.BINARY.ordinal()]='r'; // [r]aw
        FIELD_TYPE_IDENTIFIERS[FieldType.BOOLEAN.ordinal()]='b'; // [b]inary
        FIELD_TYPE_IDENTIFIERS[FieldType.DATE.ordinal()]='c'; // [c]alendar
        FIELD_TYPE_IDENTIFIERS[FieldType.DOUBLE.ordinal()]='d'; // [d]ouble
        FIELD_TYPE_IDENTIFIERS[FieldType.INSTANT.ordinal()]='g'; // [G]eorge Gamow or Admiral [G]race Hopper that were the earliest users of the term nanosecond
        FIELD_TYPE_IDENTIFIERS[FieldType.INTEGER.ordinal()]='i'; // [i]nteger
        FIELD_TYPE_IDENTIFIERS[FieldType.LONG.ordinal()]='l'; // [l]ong
        FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_OBJECT_ID.ordinal()]='x';
        FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_TIME_STAMP.ordinal()]='y';
        FIELD_TYPE_IDENTIFIERS[FieldType.NULL.ordinal()]='n'; // [n]ull
        FIELD_TYPE_IDENTIFIERS[FieldType.STRING.ordinal()]='s'; // [s]tring
        FIELD_TYPE_IDENTIFIERS[FieldType.TIME.ordinal()]='t'; // [t]ime
        FIELD_TYPE_IDENTIFIERS[FieldType.CHILD.ordinal()]='e'; // [e]lement
        
        Set<Character> fieldTypeIdentifierSet = new HashSet<>();
        for (FieldType fieldType : FieldType.values()) {
            if (FIELD_TYPE_IDENTIFIERS.length <= fieldType.ordinal()) {
                throw new SystemException("FieldType " + fieldType + " has not been mapped to an identifier.");
            }
            
            char identifier = FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()];
            
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
        }
    }

    private final IdentifierInterface identifierInterface;
    
    public IdentifierFactoryImpl(IdentifierInterface identifierInterface) {
        this.identifierInterface = identifierInterface;
    }
    
    @Override
    public String toDatabaseIdentifier(MetaSnapshot metaSnapshot, String database) {
        NameChain nameChain = new NameChain();
        nameChain.add(database);
        
        IdentifierChecker uniqueIdentifierChecker = new DatabaseIdentifierChecker(metaSnapshot);
        
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
    }
    
    @Override
    public String toCollectionIdentifier(MetaDatabase metaDatabase, String collection) {
        NameChain nameChain = new NameChain();
        nameChain.add(collection);
        
        IdentifierChecker uniqueIdentifierChecker = new TableIdentifierChecker(metaDatabase);
        
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
    }
    
    @Override
    public String toDocPartIdentifier(MetaDatabase metaDatabase, String collection, TableRef tableRef) {
        NameChain nameChain = new NameChain();
        nameChain.add(collection);
        append(nameChain, tableRef);
        
        IdentifierChecker uniqueIdentifierChecker = new TableIdentifierChecker(metaDatabase);
        
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
    }
    
    @Override
    public String toFieldIdentifier(MetaDocPart metaDocPart, FieldType fieldType, String field) {
        NameChain nameChain = new NameChain();
        nameChain.add(field);
        
        IdentifierChecker uniqueIdentifierChecker = new FieldIdentifierChecker(metaDocPart);
        
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker, String.valueOf(FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()]));
    }

    private String generateUniqueIdentifier(NameChain nameChain, IdentifierChecker uniqueIdentifierChecker) {
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker, null);
    }
    
    private String generateUniqueIdentifier(NameChain nameChain, IdentifierChecker identifierChecker, String extraImmutableName) {
        final Instant beginInstant = Instant.now();
        final int maxSize = identifierInterface.identifierMaxSize();
        String lastCollision = null;
        ChainConverterFactory chainConverterFactory = ChainConverterFactory.straight;
        Counter counter = new Counter();
        String identifier = buildIdentifier(nameChain, chainConverterFactory.createConverters(), maxSize, counter, identifierChecker, extraImmutableName);
        
        if (identifier.length() <= maxSize && identifierChecker.isUnique(identifier)) {
              return identifier;
        }
        
        if (identifier.length() <= maxSize) {
            lastCollision = identifier;
        }
        
        ChainConverterFactory counterChainConverterFactory = ChainConverterFactory.counter;
        NameConverter[] randomConverters = counterChainConverterFactory.createConverters();
        while (ChronoUnit.SECONDS.between(beginInstant, Instant.now()) < MAX_GENERATION_TIME) {
            identifier = buildIdentifier(nameChain, randomConverters, maxSize, counter, identifierChecker, extraImmutableName);
            
            if (identifier.length() > maxSize) {
                throw new SystemException("Counter generator did not fit in maxSize!");
            }
            
            if (identifierChecker.isUnique(identifier)) {
                  return identifier;
            }
            
            lastCollision = identifier;
            
            counter.increment();
        }
        
        if (lastCollision != null) {
            throw new SystemException("Identifier collision(s) does not allow to generate a valid identifier. Last collisioned identifier: " + lastCollision + ". Name chain: " + nameChain);
        }
        
        throw new SystemException("Can not generate a valid identifier. Name chain: " + nameChain);
    }
    
    private void append(NameChain nameChain, TableRef tableRef){
        if (tableRef.isRoot()){
            return;
        }
        
        TableRef parentTableRef = tableRef.getParent().get();
        String name = tableRef.getName();
        if (tableRef.isInArray()) {
            while (parentTableRef.isInArray()) {
                parentTableRef = parentTableRef.getParent().get();
            }
            name = parentTableRef.getName() + name;
            
            parentTableRef = parentTableRef.getParent().get();
        }
        
        append(nameChain, parentTableRef);
        
        nameChain.add(name);
    }
    
    private String buildIdentifier(NameChain nameChain, NameConverter[] converters, int maxSize, Counter counter, IdentifierChecker identifierChecker, String extraImmutableName) {
        final int nameMaxSize = extraImmutableName != null ? maxSize - extraImmutableName.length() - 1 : maxSize;
        StringBuilder middleIdentifierBuilder = new StringBuilder();
        
        final int size = nameChain.size();
        int index = 1;
        for (; index < size - 1; index++) {
            if (converters[1].convertWhole()) {
                middleIdentifierBuilder.append(nameChain.get(index));
            } else {
                middleIdentifierBuilder.append(converters[1].convert(nameChain.get(index), nameMaxSize, counter));
            }
            middleIdentifierBuilder.append('_');
        }
        
        StringBuilder identifierBuilder = new StringBuilder();
        
        if (converters[0].convertWhole()) {
            if (converters[0] == converters[1] && converters[0] == converters[2]) {
                StringBuilder intermediateIdentifierBuilder = new StringBuilder();
                intermediateIdentifierBuilder.append(nameChain.get(0));
                if (middleIdentifierBuilder.length() > 0) {
                    intermediateIdentifierBuilder.append(SEPARATOR);
                    intermediateIdentifierBuilder.append(middleIdentifierBuilder);
                }
                if (index < size) {
                    intermediateIdentifierBuilder.append(SEPARATOR);
                    intermediateIdentifierBuilder.append(nameChain.get(size - 1));
                }
                identifierBuilder.append(converters[0].convert(intermediateIdentifierBuilder.toString(), nameMaxSize, counter));
            } else if (converters[0] == converters[1]) {
                StringBuilder intermediateIdentifierBuilder = new StringBuilder();
                intermediateIdentifierBuilder.append(nameChain.get(0));
                if (middleIdentifierBuilder.length() > 0) {
                    intermediateIdentifierBuilder.append(SEPARATOR);
                    intermediateIdentifierBuilder.append(middleIdentifierBuilder);
                }
                identifierBuilder.append(converters[0].convert(intermediateIdentifierBuilder.toString(), nameMaxSize, counter));
                if (index < size) {
                    identifierBuilder.append(SEPARATOR);
                    identifierBuilder.append(converters[2].convert(nameChain.get(size - 1), nameMaxSize, counter));
                }
            } else {
                identifierBuilder.append(converters[0].convert(nameChain.get(0), nameMaxSize, counter));
                if (middleIdentifierBuilder.length() > 0) {
                    identifierBuilder.append(SEPARATOR);
                    identifierBuilder.append(middleIdentifierBuilder);
                }
                if (index < size) {
                    identifierBuilder.append(SEPARATOR);
                    identifierBuilder.append(converters[2].convert(nameChain.get(size - 1), nameMaxSize, counter));
                }
            }
        } else if (converters[1].convertWhole()) {
            if (converters[1] == converters[2]) {
                identifierBuilder.append(converters[0].convert(nameChain.get(0), nameMaxSize, counter));
                StringBuilder intermediateIdentifierBuilder = new StringBuilder();
                if (middleIdentifierBuilder.length() > 0) {
                    intermediateIdentifierBuilder.append(middleIdentifierBuilder);
                }
                if (index < size) {
                    intermediateIdentifierBuilder.append(SEPARATOR);
                    intermediateIdentifierBuilder.append(nameChain.get(size - 1));
                }
                if (intermediateIdentifierBuilder.length() > 0) {
                    identifierBuilder.append(SEPARATOR);
                    identifierBuilder.append(converters[1].convert(intermediateIdentifierBuilder.toString(), nameMaxSize, counter));
                }
            } else {
                identifierBuilder.append(converters[0].convert(nameChain.get(0), nameMaxSize, counter));
                if (middleIdentifierBuilder.length() > 0) {
                    identifierBuilder.append(SEPARATOR);
                    identifierBuilder.append(converters[1].convert(middleIdentifierBuilder.toString(), nameMaxSize, counter));
                }
                if (index < size) {
                    identifierBuilder.append(SEPARATOR);
                    identifierBuilder.append(nameChain.get(size - 1));
                }
            }
        } else {
            identifierBuilder.append(converters[0].convert(nameChain.get(0), nameMaxSize, counter));
            if (middleIdentifierBuilder.length() > 0) {
                identifierBuilder.append(SEPARATOR);
                identifierBuilder.append(middleIdentifierBuilder);
            }
            if (index < size) {
                identifierBuilder.append(SEPARATOR);
                identifierBuilder.append(nameChain.get(size - 1));
            }
        }
        
        if (extraImmutableName != null) {
            identifierBuilder.append(SEPARATOR).append(extraImmutableName);
        }
        
        String identifier = identifierBuilder.toString();
        
        if (!identifierChecker.isAllowed(identifierInterface, identifier)) {
            identifier = SEPARATOR + identifier;
        }
        
        return identifier;
    }
    
    private static class NameChain {
        private final static Pattern NO_ALLOWED_CHAR_PATTERN = Pattern.compile("[^0-9a-z_]");
        private final ArrayList<String> names;
        
        public NameChain() {
            names = new ArrayList<>();
        }
        
        public void add(String e) {
            e = Normalizer.normalize(e, Normalizer.Form.NFD);
            e = NO_ALLOWED_CHAR_PATTERN.matcher(e
                .toLowerCase(Locale.US))
                    .replaceAll(SEPARATOR_STRING);
            
            names.add(e);
        }

        public String get(int index) {
            return names.get(index);
        }
        
        public int size() {
            return names.size();
        }
    }
    
    private static enum ChainConverterFactory {
        straight(NameConverterFactory.straight),
        straight_cutvowels(NameConverterFactory.straight, NameConverterFactory.cutvowels, NameConverterFactory.straight),
        straight_singlechar(NameConverterFactory.straight, NameConverterFactory.singlechar, NameConverterFactory.straight),
        straight_hash(NameConverterFactory.straight, NameConverterFactory.hash, NameConverterFactory.straight),
        first_straight_hash(NameConverterFactory.straight, NameConverterFactory.hash),
        cutvowels(NameConverterFactory.cutvowels),
        cutvowels_singlechar(NameConverterFactory.cutvowels, NameConverterFactory.singlechar, NameConverterFactory.cutvowels),
        cutvowels_hash(NameConverterFactory.cutvowels, NameConverterFactory.hash, NameConverterFactory.cutvowels),
        first_cutvowels_hash(NameConverterFactory.cutvowels, NameConverterFactory.hash),
        hash(NameConverterFactory.hash),
        hash_and_random(NameConverterFactory.hash_and_random),
        counter(NameConverterFactory.counter);
        
        private final NameConverterFactory[] converterFactories;
        
        private ChainConverterFactory(NameConverterFactory...nameConverterFactories) {
            this.converterFactories = nameConverterFactories;
        }
        
        public NameConverter[] createConverters() {
            NameConverter[] converters = new NameConverter[3];
            
            converters[0] = converterFactories[0].create();
            converters[1] = converters[0];
            converters[2] = converters[0];
            
            if (converterFactories.length > 1) {
                if (converterFactories[0] == converterFactories[1]) {
                    converters[1] = converters[0];
                } else {
                    converters[1] = converterFactories[1].create();
                }
                
                converters[2] = converters[0];
            }
            
            if (converterFactories.length > 2) {
                if (converterFactories[0] == converterFactories[2]) {
                    converters[2] = converters[0];
                } else if (converterFactories[1] == converterFactories[2]) {
                    converters[2] = converters[1];
                } else {
                    converters[2] = converterFactories[2].create();
                }
            }
            
            return converters;
        }
    }
    
    private static enum NameConverterFactory {
        straight {
            private final NameConverter nameConverter = new NameConverter() {
                @Override
                public String convert(String name, int maxSize, Counter counter) {
                    return name;
                }
            };
            
            public NameConverter create() {
                return nameConverter;
            }
        },
        cutvowels {
            public NameConverter create() {
                return new NameConverter() {
                    private final Pattern pattern = Pattern.compile("([a-z])[aeiou]+");
                    
                    @Override
                    public String convert(String name, int maxSize, Counter counter) {
                        return pattern.matcher(name).replaceAll("$1");
                    }
                };
            }
        },
        singlechar {
            private final NameConverter nameConverter = new NameConverter() {
                @Override
                public String convert(String name, int maxSize, Counter counter) {
                    if (name.isEmpty()) {
                        return name;
                    }
                    
                    return "" + name.charAt(0);
                }
            };
            
            public NameConverter create() {
                return nameConverter;
            }
        },
        hash {
            private final NameConverter nameConverter = new NameConverter() {
                @Override
                public String convert(String name, int maxSize, Counter counter) {
                    String value = SEPARATOR + 'x' + Integer.toHexString(name.hashCode());
                    
                    if (name.length() + value.length() < maxSize) {
                        return name + value;
                    }
                    
                    int availableSize = Math.min(name.length(), maxSize) - value.length();
                    return name.substring(0, availableSize / 2 + availableSize % 2) 
                            + name.substring(name.length() - availableSize / 2, name.length())
                            + value;
                }
                
                @Override
                public boolean convertWhole() {
                    return true;
                }
            };
            
            public NameConverter create() {
                return nameConverter;
            }
        },
        hash_and_random {
            private final NameConverter nameConverter = new NameConverter() {
                private final Random random = new Random();
                
                @Override
                public String convert(String name, int maxSize, Counter counter) {
                    String value = SEPARATOR + 'x' + Integer.toHexString(name.hashCode())
                        + SEPARATOR + 'r' + Integer.toHexString(random.nextInt());
                    
                    if (name.length() + value.length() < maxSize) {
                        return name + value;
                    }
                    
                    int availableSize = Math.min(name.length(), maxSize) - value.length();
                    return name.substring(0, availableSize / 2 + availableSize % 2) 
                            + name.substring(name.length() - availableSize / 2, name.length())
                            + value;
                }
                
                @Override
                public boolean convertWhole() {
                    return true;
                }
            };
            
            public NameConverter create() {
                return nameConverter;
            }
        },
        counter {
            private final NameConverter nameConverter = new NameConverter() {
                @Override
                public String convert(String name, int maxSize, Counter counter) {
                    String value = SEPARATOR + String.valueOf(counter.get());
                    
                    if (name.length() + value.length() < maxSize) {
                        return name + value;
                    }
                    
                    int availableSize = Math.min(name.length(), maxSize) - value.length();
                    return name.substring(0, availableSize / 2 + availableSize % 2) 
                            + name.substring(name.length() - availableSize / 2, name.length())
                            + value;
                }
                
                @Override
                public boolean convertWhole() {
                    return true;
                }
            };
            
            public NameConverter create() {
                return nameConverter;
            }
        };
        
        public abstract NameConverter create();
    }
    
    private abstract static class NameConverter {
        public abstract String convert(String name, int maxSize, Counter counter);
        public boolean convertWhole() {
            return false;
        }
    }
    
    private static class Counter {
        private int counter = 1;
        
        public int get() {
            return counter;
        }
        
        public void increment() {
            counter++;
        }
    }
    
    private static interface IdentifierChecker {
        boolean isUnique(String identifier);
        boolean isAllowed(IdentifierInterface identifierInterface, String identifier);
    }
    
    private static class DatabaseIdentifierChecker implements IdentifierChecker {
        private final MetaSnapshot metaSnapshot;
        
        public DatabaseIdentifierChecker(MetaSnapshot metaSnapshot) {
            super();
            this.metaSnapshot = metaSnapshot;
        }
        
        @Override
        public boolean isUnique(String identifier) {
            return metaSnapshot.getMetaDatabaseByIdentifier(identifier) == null;
        }

        @Override
        public boolean isAllowed(IdentifierInterface identifierInterface, String identifier) {
            return identifierInterface.isAllowedSchemaIdentifier(identifier);
        }
    }
    
    private static class TableIdentifierChecker implements IdentifierChecker {
        private final MetaDatabase metaDatabase;
        
        public TableIdentifierChecker(MetaDatabase metaDatabase) {
            super();
            this.metaDatabase = metaDatabase;
        }
        
        @Override
        public boolean isUnique(String identifier) {
            Iterator<? extends MetaCollection> metaCollectionIterator = metaDatabase.streamMetaCollections().iterator();
            
            while (metaCollectionIterator.hasNext()){
                MetaCollection metaCollection = metaCollectionIterator.next();
                if (metaCollection.getMetaDocPartByIdentifier(identifier) != null) {
                    return false;
                }
            }
            
            return true;
        }

        @Override
        public boolean isAllowed(IdentifierInterface identifierInterface, String identifier) {
            return identifierInterface.isAllowedTableIdentifier(identifier);
        }
    }
    
    private static class FieldIdentifierChecker implements IdentifierChecker {
        private final MetaDocPart metaDocPart;
        
        public FieldIdentifierChecker(MetaDocPart metaDocPart) {
            super();
            this.metaDocPart = metaDocPart;
        }
        
        @Override
        public boolean isUnique(String identifier) {
            return metaDocPart.getMetaFieldByIdentifier(identifier) == null;
        }

        @Override
        public boolean isAllowed(IdentifierInterface identifierInterface, String identifier) {
            return identifierInterface.isAllowedColumnIdentifier(identifier);
        }
    }
    
    public interface IdentifierInterface {
        @Nonnull int identifierMaxSize();
        @Nonnull boolean isAllowedSchemaIdentifier(@Nonnull String identifier);
        @Nonnull boolean isAllowedTableIdentifier(@Nonnull String identifier);
        @Nonnull boolean isAllowedColumnIdentifier(@Nonnull String identifier);
    }
}
