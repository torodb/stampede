
package com.torodb.core.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * This annotation is used annotate resources, specially {@link ThreadFactory thread factories} and
 * {@link Executor executors} used by classes that inherit {@link ThreadFactoryIdleService}.
 */
@Qualifier
@Target({ FIELD, PARAMETER, METHOD, TYPE })
@Retention(RUNTIME)
@Documented
public @interface ToroDbIdleService {

}
