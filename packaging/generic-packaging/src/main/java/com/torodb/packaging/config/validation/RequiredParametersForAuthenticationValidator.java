/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.packaging.config.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.torodb.packaging.config.model.protocol.mongo.Auth;

public class RequiredParametersForAuthenticationValidator implements ConstraintValidator<RequiredParametersForAuthentication, Auth> {
	
	@Override
	public void initialize(RequiredParametersForAuthentication constraintAnnotation) {
	}

	@Override
	public boolean isValid(Auth value, ConstraintValidatorContext context) {
	    switch (value.getMode()) {
        case cr:
        case negotiate:
        case scram_sha1:
            if (value.getUser() == null ||
                    value.getSource() == null) {
                return false;
            }
            break;
        case x509:
        case disabled:
        default:
            break;
	    }

		return true;
	}
}