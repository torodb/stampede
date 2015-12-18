
package com.torodb.torod.mongodb.translator;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.Sets;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.mongodb.exp.modifiers.ExpModifier;
import java.util.HashSet;

/**
 *
 */
public class NotConsumedExpModifierException extends UserToroException {
    private static final long serialVersionUID = 1L;

    private final HashSet<ExpModifier> notConsumedModifiers;

    public NotConsumedExpModifierException(ClassToInstanceMap<ExpModifier> notConsumedModifiers) {
        super("The following expression modifiers were not consumed:" + notConsumedModifiers.values());
        this.notConsumedModifiers = Sets.newHashSet(notConsumedModifiers.values());
    }

    public HashSet<ExpModifier> getNotConsumedModifiers() {
        return notConsumedModifiers;
    }
    
}
