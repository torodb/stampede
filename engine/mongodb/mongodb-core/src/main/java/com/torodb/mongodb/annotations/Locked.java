/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.annotation.concurrent.GuardedBy;

/**
 * This documentation annotation is used to inform that the annotated method should be
 * thread-protected by a lock.
 *
 * <p/>By default, the lock that has to be used is implicit by the context. If there is not clear 
 * which lock should be used, it can be specified with {@link GuardedBy} annotation.
 *
 * <p/>The {@link #exclusive() } property informs if the lock must be exclusive or not.
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
