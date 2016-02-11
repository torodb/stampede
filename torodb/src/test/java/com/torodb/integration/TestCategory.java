/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with torodb. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.integration;

/**
 *
 */
public enum TestCategory {
    /**
     * Test that must success in ToroDB.
     *//**
     * Test that must success in ToroDB.
     */
    WORKING,
    /**
     * Test that must not success in ToroDB.
     *
     * A correct implementation of ToroDB should fail when this kind of tests are executed.
     */
    FAILING,
    /**
     * Test that, when executed, freezes ToroDB or lets it in a inconsistent state.
     *
     * On a correct implementation of ToroDB should be impossible to find a test that do it
     */
    CATASTROPHIC,
    /**
     * Test that should be on {@link #FAILING} but succeed.
     *
     * They represents a inconsisten behaviour of ToroDB that should be removed in following versions.
     */
    FALSE_POSITIVE,
    /**
     * Tests that fail in the current implementation, but we know we want to implement in the
     * following versions, so they should not be on {@link #FAILING}.
     */
    NOT_IMPLEMENTED,
    /**
     * Tets that we are not interested in.
     *
     * They might represent MongoDB specific features that we don't want to implement
     */
    IGNORED

}
