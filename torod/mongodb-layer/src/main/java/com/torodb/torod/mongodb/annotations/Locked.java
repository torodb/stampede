
package com.torodb.torod.mongodb.annotations;

import java.lang.annotation.*;
import javax.annotation.concurrent.GuardedBy;

/**
 * This documentation annotation is used to inform that the annotated method
 * should be thread-protected by a lock.
 * <p/>
 * By default, the lock that has to be used is implicit by the context. If there
 * is not clear which lock should be used, it can be specified with 
 * {@link GuardedBy} annotation.
 * <p/>
 * The {@link #exclusive() } property informs if the lock must be exclusive or
 * not.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface Locked {

    /**
     * @return if the given method must be executed in a exclusive lock
     */
    public boolean exclusive() default false;
}
