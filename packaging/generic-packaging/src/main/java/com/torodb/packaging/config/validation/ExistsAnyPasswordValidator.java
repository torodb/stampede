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

import com.torodb.packaging.config.model.backend.BackendPasswordConfig;

public class ExistsAnyPasswordValidator implements ConstraintValidator<ExistsAnyPassword, BackendPasswordConfig> {
	
	@Override
	public void initialize(ExistsAnyPassword constraintAnnotation) {
	}

	@Override
	public boolean isValid(BackendPasswordConfig value, ConstraintValidatorContext context) {
		return value == null || value.getPassword() != null || value.getToropassFile() != null;
	}
}
