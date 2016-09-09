
package com.torodb.core.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * This annotation is used annotate a objects (usually handlers) that executes one or several
 * methods on several threads.
 *
 * It is important to say that objects injected with this annotation are <b>not required</b> to be
 * thread safe.
 */
@Qualifier
@Target({ FIELD, PARAMETER, METHOD, TYPE })
@Retention(RUNTIME)
@Documented
public @interface UseThreads {

}
