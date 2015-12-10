package com.torodb.config.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.torodb.config.model.backend.Password;

public class AnyPasswordValidator implements ConstraintValidator<AnyPassword, Password> {
	
	@Override
	public void initialize(AnyPassword constraintAnnotation) {
	}

	@Override
	public boolean isValid(Password value, ConstraintValidatorContext context) {
		return value == null || value.getPassword() != null || value.getToropassFile() != null;
	}
}
