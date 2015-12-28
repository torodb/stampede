
package com.torodb.torod.mongodb.exp.modifiers;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;
import com.google.common.collect.MutableClassToInstanceMap;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.bson.BsonDocument;

/**
 *
 */
public class ExpModifications {

    public static final ExpModifications NO_MODIFICATIONS = new ExpModifications();

    private final ImmutableClassToInstanceMap<ExpModifier> modifiers;
    private final Set<Class> consumed;
    
    private ExpModifications() {
        modifiers = new ImmutableClassToInstanceMap.Builder<ExpModifier>().build();
        consumed = Collections.emptySet();
    }
    
    public ExpModifications(BsonDocument queryNode) {
        ImmutableClassToInstanceMap.Builder<ExpModifier> builder
                = ImmutableClassToInstanceMap.<ExpModifier>builder();
        
        RegexExpModifier regexModifier = RegexExpModifier.fromBSON(queryNode);
        if (regexModifier != null) {
            builder.put(RegexExpModifier.class, regexModifier);
        }
        modifiers = builder.build();
        this.consumed = Sets.newHashSetWithExpectedSize(modifiers.size());
    }
    
    public static boolean isExpModifer(String key) {
        return RegexExpModifier.isExpModifier(key);
    }
    
    /**
     * Gets a modifier and marks it as consumed
     * @param <E>
     * @param clazz
     * @return 
     */
    @Nullable
    public <E extends ExpModifier> E consumeModifier(Class<E> clazz) {
        E instance = modifiers.getInstance(clazz);
        if (instance != null) {
            markAsConsumed(clazz);
        }
        return instance;
    }

    private <E extends ExpModifier> void markAsConsumed(Class<E> clazz) {
        assert modifiers.containsKey(clazz);
        consumed.add(clazz);
    }
    
    public ClassToInstanceMap<ExpModifier> getNotConsumedModifiers() {
        ClassToInstanceMap<ExpModifier> mutable = MutableClassToInstanceMap.create();
        
        for (Map.Entry<Class<? extends ExpModifier>, ExpModifier> entrySet : modifiers.entrySet()) {
            if (!consumed.contains(entrySet.getKey())) {
                mutable.put(entrySet.getKey(), entrySet.getValue());
            }
        }
        return ImmutableClassToInstanceMap.copyOf(mutable);
    }
    
}
