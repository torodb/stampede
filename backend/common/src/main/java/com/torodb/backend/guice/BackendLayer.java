
package com.torodb.backend.guice;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * This annotation is used identify resources that are related to the backend layer
 */
@Qualifier
@Target({ FIELD, PARAMETER, METHOD, TYPE })
@Retention(RUNTIME)
@Documented
public @interface BackendLayer {

}
