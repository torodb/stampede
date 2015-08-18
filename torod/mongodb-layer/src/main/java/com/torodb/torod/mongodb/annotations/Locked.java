
package com.torodb.torod.mongodb.annotations;

import java.lang.annotation.*;

/**
 *
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
