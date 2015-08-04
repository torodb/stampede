
package com.torodb.util.mgl;

/**
 *
 */
public enum Mode {
    /**
     * Intention to share mode.
     * Incompatible with:
     * <ol>
     * <li>X</li>
     * </ol>
     */
    IS,
    /**
     * Intention to exclusive mode.
     * Incompatible with:
     * <ol>
     * <li>S</li>
     * <li>X</li>
     * </ol>
     */
    IX,
    /**
     * Shared mode.
     * Incompatible with:
     * <ol>
     * <li>IX</li>
     * <li>X</li>
     * </ol>
     */
    S,
    /**
     * Exclusive mode.
     * Incompatible with:
     * <ol>
     * <li>IS</li>
     * <li>IX</li>
     * <li>S</li>
     * <li>X</li>
     * </ol>
     */
    X
}
