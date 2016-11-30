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

package com.torodb.core.annotations;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Elements annotated by this annotation cannot change.
 *
 * When it annotates a method, it means that the result must not be changed after the method
 * execution. When it annotates a parameter, it means that only objects that won't be changed will
 * be passed as argument.
 *
 * Usually, annotated objects should be immutable or be defensively copied, but sometimes it is not
 * possible for performance reasons.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({PARAMETER, METHOD})
@Documented
public @interface DoNotChange {

}
