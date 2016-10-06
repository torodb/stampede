
package com.torodb.mongodb.repl.guice;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * This annotation identifies the name of the repl set this instance is replicating from.
 *
 * It must annotate a String and it must be the same on the whole replication injector.
 */
@Qualifier
@Target({ FIELD, PARAMETER, METHOD, TYPE })
@Retention(RUNTIME)
@Documented
public @interface ReplSetName {

}
