
package com.torodb.mongodb.repl.topology;

import com.google.common.net.HostAndPort;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * This annotation is used annotate a {@link HostAndPort}, so it can be
 * identified as the addres from which the initial replication configuration
 * must be retrieve.
 */
@Qualifier
@Target({ FIELD, PARAMETER, METHOD, TYPE })
@Retention(RUNTIME)
@Documented
public @interface RemoteSeed {

}
