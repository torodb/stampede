package com.torodb.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.metainf.*;
import java.text.Normalizer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;
import java.util.regex.Pattern;

public class IdentifierFactoryImpl implements IdentifierFactory {

    private static final int MAX_GENERATION_TIME = 10;

    private final IdentifierConstraints identifierConstraints;
    private final char separator;
    private final String separatorString;
    private final char arrayDimensionSeparator;
    
    public IdentifierFactoryImpl(IdentifierConstraints identifierConstraints) {
        this.identifierConstraints = identifierConstraints;
        this.separator = identifierConstraints.getSeparator();
        this.separatorString = String.valueOf(separator);
        this.arrayDimensionSeparator = identifierConstraints.getArrayDimensionSeparator();
    }
    
    @Override
    public String toDatabaseIdentifier(MetaSnapshot metaSnapshot, String database) {
        NameChain nameChain = new NameChain(separatorString);
        nameChain.add(database);
        
        IdentifierChecker uniqueIdentifierChecker = new DatabaseIdentifierChecker(metaSnapshot);
        
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
    }
    
    @Override
    public String toCollectionIdentifier(MetaDatabase metaDatabase, String collection) {
        NameChain nameChain = new NameChain(separatorString);
        nameChain.add(collection);
        
        IdentifierChecker uniqueIdentifierChecker = new TableIdentifierChecker(metaDatabase);
        
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
    }
    
    @Override
    public String toDocPartIdentifier(MetaDatabase metaDatabase, String collection, TableRef tableRef) {
        NameChain nameChain = new NameChain(separatorString);
        nameChain.add(collection);
        append(nameChain, tableRef);
        
        IdentifierChecker uniqueIdentifierChecker = new TableIdentifierChecker(metaDatabase);
        
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
    }
    
    @Override
    public String toFieldIdentifier(MetaDocPart metaDocPart, FieldType fieldType, String field) {
        NameChain nameChain = new NameChain(separatorString);
        nameChain.add(field);
        
        IdentifierChecker uniqueIdentifierChecker = new FieldIdentifierChecker(metaDocPart);
        
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker, String.valueOf(identifierConstraints.getFieldTypeIdentifier(fieldType)));
    }
    
    @Override
    public String toFieldIdentifierForScalar(FieldType fieldType) {
        return identifierConstraints.getScalarIdentifier(fieldType);
    }

    private String generateUniqueIdentifier(NameChain nameChain, IdentifierChecker uniqueIdentifierChecker) {
        return generateUniqueIdentifier(nameChain, uniqueIdentifierChecker, null);
    }
    
    private String generateUniqueIdentifier(NameChain nameChain, IdentifierChecker identifierChecker, String extraImmutableName) {
        final Instant beginInstant = Instant.now();
        final int maxSize = identifierConstraints.identifierMaxSize();
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
            name = parentTableRef.getName() + arrayDimensionSeparator + tableRef.getArrayDimension();
            
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
        for (; index < size - 2; index++) {
            if (converters[1].convertWhole()) {
                middleIdentifierBuilder.append(nameChain.get(index));
            } else {
                middleIdentifierBuilder.append(converters[1].convert(nameChain.get(index), separator, nameMaxSize, counter));
            }
            middleIdentifierBuilder.append('_');
        }
        if (index < size - 1) {
            if (converters[1].convertWhole()) {
                middleIdentifierBuilder.append(nameChain.get(index));
            } else {
                middleIdentifierBuilder.append(converters[1].convert(nameChain.get(index), separator, nameMaxSize, counter));
            }
            index++;
        }
        
        StringBuilder identifierBuilder = new StringBuilder();
        
        if (converters[0].convertWhole()) {
            if (converters[0] == converters[1] && converters[0] == converters[2]) {
                StringBuilder intermediateIdentifierBuilder = new StringBuilder();
                intermediateIdentifierBuilder.append(nameChain.get(0));
                if (middleIdentifierBuilder.length() > 0) {
                    intermediateIdentifierBuilder.append(separator);
                    intermediateIdentifierBuilder.append(middleIdentifierBuilder);
                }
                if (index < size) {
                    intermediateIdentifierBuilder.append(separator);
                    intermediateIdentifierBuilder.append(nameChain.get(size - 1));
                }
                identifierBuilder.append(converters[0].convert(intermediateIdentifierBuilder.toString(), separator, nameMaxSize, counter));
            } else if (converters[0] == converters[1]) {
                StringBuilder intermediateIdentifierBuilder = new StringBuilder();
                intermediateIdentifierBuilder.append(nameChain.get(0));
                if (middleIdentifierBuilder.length() > 0) {
                    intermediateIdentifierBuilder.append(separator);
                    intermediateIdentifierBuilder.append(middleIdentifierBuilder);
                }
                identifierBuilder.append(converters[0].convert(intermediateIdentifierBuilder.toString(), separator, nameMaxSize, counter));
                if (index < size) {
                    identifierBuilder.append(separator);
                    identifierBuilder.append(converters[2].convert(nameChain.get(size - 1), separator, nameMaxSize, counter));
                }
            } else {
                identifierBuilder.append(converters[0].convert(nameChain.get(0), separator, nameMaxSize, counter));
                if (middleIdentifierBuilder.length() > 0) {
                    identifierBuilder.append(separator);
                    identifierBuilder.append(middleIdentifierBuilder);
                }
                if (index < size) {
                    identifierBuilder.append(separator);
                    identifierBuilder.append(converters[2].convert(nameChain.get(size - 1), separator, nameMaxSize, counter));
                }
            }
        } else if (converters[1].convertWhole()) {
            if (converters[1] == converters[2]) {
                identifierBuilder.append(converters[0].convert(nameChain.get(0), separator, nameMaxSize, counter));
                StringBuilder intermediateIdentifierBuilder = new StringBuilder();
                if (middleIdentifierBuilder.length() > 0) {
                    intermediateIdentifierBuilder.append(middleIdentifierBuilder);
                }
                if (index < size) {
                    intermediateIdentifierBuilder.append(separator);
                    intermediateIdentifierBuilder.append(nameChain.get(size - 1));
                }
                if (intermediateIdentifierBuilder.length() > 0) {
                    identifierBuilder.append(separator);
                    identifierBuilder.append(converters[1].convert(intermediateIdentifierBuilder.toString(), separator, nameMaxSize, counter));
                }
            } else {
                identifierBuilder.append(converters[0].convert(nameChain.get(0), separator, nameMaxSize, counter));
                if (middleIdentifierBuilder.length() > 0) {
                    identifierBuilder.append(separator);
                    identifierBuilder.append(converters[1].convert(middleIdentifierBuilder.toString(), separator, nameMaxSize, counter));
                }
                if (index < size) {
                    identifierBuilder.append(separator);
                    identifierBuilder.append(nameChain.get(size - 1));
                }
            }
        } else {
            identifierBuilder.append(converters[0].convert(nameChain.get(0), separator, nameMaxSize, counter));
            if (middleIdentifierBuilder.length() > 0) {
                identifierBuilder.append(separator);
                identifierBuilder.append(middleIdentifierBuilder);
            }
            if (index < size) {
                identifierBuilder.append(separator);
                identifierBuilder.append(nameChain.get(size - 1));
            }
        }
        
        if (extraImmutableName != null) {
            identifierBuilder.append(separator).append(extraImmutableName);
        }
        
        String identifier = identifierBuilder.toString();
        
        if (!identifierChecker.isAllowed(identifierConstraints, identifier)) {
            identifier = separator + identifier;
        }
        
        return identifier;
    }
    
    private static class NameChain {
        private final static Pattern NO_ALLOWED_CHAR_PATTERN = Pattern.compile("[^0-9a-z_$]");
        private final String separatorString;
        private final ArrayList<String> names;
        
        public NameChain(String separatorString) {
            this.separatorString = separatorString;
            names = new ArrayList<>();
        }
        
        public void add(String e) {
            e = Normalizer.normalize(e, Normalizer.Form.NFD);
            e = NO_ALLOWED_CHAR_PATTERN.matcher(e
                .toLowerCase(Locale.US))
                    .replaceAll(separatorString);
            
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
                public String convert(String name, char separator, int maxSize, Counter counter) {
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
                    public String convert(String name, char separator, int maxSize, Counter counter) {
                        return pattern.matcher(name).replaceAll("$1");
                    }
                };
            }
        },
        singlechar {
            private final NameConverter nameConverter = new NameConverter() {
                @Override
                public String convert(String name, char separator, int maxSize, Counter counter) {
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
                public String convert(String name, char separator, int maxSize, Counter counter) {
                    String value = separator + 'x' + Integer.toHexString(name.hashCode());
                    
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
                public String convert(String name, char separator, int maxSize, Counter counter) {
                    String value = separator + 'x' + Integer.toHexString(name.hashCode())
                        + separator + 'r' + Integer.toHexString(random.nextInt());
                    
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
                public String convert(String name, char separator, int maxSize, Counter counter) {
                    String value = separator + String.valueOf(counter.get());
                    
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
        public abstract String convert(String name, char separator, int maxSize, Counter counter);
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
        boolean isAllowed(IdentifierConstraints identifierInterface, String identifier);
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
        public boolean isAllowed(IdentifierConstraints identifierInterface, String identifier) {
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
        public boolean isAllowed(IdentifierConstraints identifierInterface, String identifier) {
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
        public boolean isAllowed(IdentifierConstraints identifierInterface, String identifier) {
            return identifierInterface.isAllowedColumnIdentifier(identifier);
        }
    }
}
