package com.torodb.config.validation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.torodb.config.model.protocol.mongo.Replication;

public class NoDuplicatedReplNameValidator implements ConstraintValidator<NoDuplicatedReplName, List<Replication>> {
	
	@Override
	public void initialize(NoDuplicatedReplName constraintAnnotation) {
	}

	@Override
	public boolean isValid(List<Replication> value, ConstraintValidatorContext context) {
		if (value != null) {
			Set<String> replNameSet = new HashSet<String>();
			for (Replication replication : value) {
				if (!replNameSet.add(replication.getReplSetName())) {
					return false;
				}
			}
		}

		return true;
	}
}
