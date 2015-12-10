package com.torodb.config.validation;

import java.util.List;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class NotNullElementsValidator implements ConstraintValidator<NotNullElements, List<?>> {
	
	@Override
	public void initialize(NotNullElements constraintAnnotation) {
	}

	@Override
	public boolean isValid(List<?> value, ConstraintValidatorContext context) {
		if (value != null) {
			for (Object element : value) {
				if (element == null) {
					return false;
				}
			}
		}

		return true;
	}
}
