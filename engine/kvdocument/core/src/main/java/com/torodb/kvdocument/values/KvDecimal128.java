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

package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.Decimal128Type;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;


public abstract class KvDecimal128 extends KvValue<KvDecimal128> {

  private static final long serialVersionUID = 6351251976353558479L;

  public static KvDecimal128 of(long high, long low) {
    return new DefaultKvDecimal128(high, low);
  }

  public static KvDecimal128 of(BigDecimal value) {
    return new DefaultKvDecimal128(value);
  }

  public static KvDecimal128 NaN = DefaultKvDecimal128.NaN;
  public static KvDecimal128 INFINITY = DefaultKvDecimal128.INFINITY;
  public static KvDecimal128 NEGATIVE_ZERO = DefaultKvDecimal128.NEGATIVE_ZERO;

  @Nonnull
  @Override
  public KvDecimal128 getValue() {
    return this;
  }

  @Nonnull
  @Override
  public Class<? extends KvDecimal128> getValueClass() {
    return KvDecimal128.class;
  }

  @Override
  public Decimal128Type getType() {
    return Decimal128Type.INSTANCE;
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }
    if (null == obj) {
      return false;
    }
    if (!(obj instanceof KvDecimal128)) {
      return false;
    }

    KvDecimal128 dec = (KvDecimal128) obj;

    if (getLow() != dec.getLow()) {
      return false;
    }

    if (getHigh() != dec.getHigh()) {
      return false;
    }

    return true;
  
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  public abstract boolean isNaN();

  public abstract boolean isInfinite();

  public abstract boolean isNegativeZero();

  public abstract BigDecimal getBigDecimal();

  public abstract long getHigh();

  public abstract long getLow();

  private static class DefaultKvDecimal128 extends KvDecimal128 {

    private static final long serialVersionUID = 1946360116636891512L;

    private static final long INFINITY_MASK = 0x7800000000000000L;
    private static final long NaN_MASK = 0x7c00000000000000L;
    private static final long SIGN_BIT_MASK = 1L << 63;
    private static final int EXPONENT_OFFSET = 6176;
    private static final int MIN_EXPONENT = -6176;
    private static final int MAX_EXPONENT = 6111;
    private static final int MAX_BIT_LENGTH = 113;
    
    private static final BigInteger BIG_INT_TEN = new BigInteger("10");
    private static final BigInteger BIG_INT_ONE = new BigInteger("1");
    private static final BigInteger BIG_INT_ZERO = new BigInteger("0");


    private static final DefaultKvDecimal128 NaN = new DefaultKvDecimal128(NaN_MASK, 0);
    private static final DefaultKvDecimal128 INFINITY = new DefaultKvDecimal128(INFINITY_MASK, 0);
    private static final DefaultKvDecimal128 NEGATIVE_ZERO = new DefaultKvDecimal128(0xb040000000000000L, 0x0000000000000000L);
    private long high;
    private long low;

    private DefaultKvDecimal128(long high, long low) {
      this.high = high;
      this.low = low;
    }

    private DefaultKvDecimal128(BigDecimal value) {
      bigDecimalToDecimal128(value, value.signum() == -1);
    }

    @Override
    public boolean isNaN() {
      return (high & NaN_MASK) == NaN_MASK;
    }

    @Override
    public boolean isInfinite() {
      return (high & INFINITY_MASK) == INFINITY_MASK;
    }

    @Override
    public boolean isNegativeZero(){
      return isNegative() && getBigDecimal().signum() == 0;
    }

    @Override
    public BigDecimal getBigDecimal() {
      int scale = -getExponent();

      if (twoHighestCombinationBitsAreSet()) {
        return BigDecimal.valueOf(0, scale);
      }

      return new BigDecimal(new BigInteger(isNegative() ? -1 : 1, getBytes()), scale);
    }

    // May have leading zeros.
    private byte[] getBytes() {
      byte[] bytes = new byte[15];

      long mask = 0x00000000000000ff;
      for (int i = 14; i >= 7; i--) {
        bytes[i] = (byte) ((low & mask) >>> ((14 - i) << 3));
        mask = mask << 8;
      }

      mask = 0x00000000000000ff;
      for (int i = 6; i >= 1; i--) {
        bytes[i] = (byte) ((high & mask) >>> ((6 - i) << 3));
        mask = mask << 8;
      }

      mask = 0x0001000000000000L;
      bytes[0] = (byte) ((high & mask) >>> 48);
      return bytes;
    }

    private int getExponent() {
      if (twoHighestCombinationBitsAreSet()) {
        return (int) ((high & 0x1fffe00000000000L) >>> 47) - EXPONENT_OFFSET;
      } else {
        return (int) ((high & 0x7fff800000000000L) >>> 49) - EXPONENT_OFFSET;
      }
    }

    private boolean twoHighestCombinationBitsAreSet() {
      return (high & 3L << 61) == 3L << 61;
    }

    /**
     * Returns true if this Decimal128 is negative.
     *
     * @return true if this Decimal128 is negative
     */
    private boolean isNegative() {
      return (high & SIGN_BIT_MASK) == SIGN_BIT_MASK;
    }

    /**
     * Convert a bigDecimal in two longs which represent a Decimal128
     * @param initialValue bigDecimal to convert
     * @param isNegative boolean necessary to detect -0, 
     *          which can't be represented with a BigDecimal 
     */
    private void bigDecimalToDecimal128(final BigDecimal initialValue, final boolean isNegative) {
      long localHigh = 0;
      long localLow = 0;

      BigDecimal value = clampAndRound(initialValue);

      long exponent = -value.scale();

      if ((exponent < MIN_EXPONENT) || (exponent > MAX_EXPONENT)) {
        throw new AssertionError("Exponent is out of range for Decimal128 encoding: " + exponent);
      }

      if (value.unscaledValue().bitLength() > MAX_BIT_LENGTH) {
        throw new AssertionError("Unscaled roundedValue is out of range for Decimal128 encoding:"
            + value.unscaledValue());
      }

      BigInteger significand = value.unscaledValue().abs();
      int bitLength = significand.bitLength();

      for (int i = 0; i < Math.min(64, bitLength); i++) {
        if (significand.testBit(i)) {
          localLow |= 1L << i;
        }
      }

      for (int i = 64; i < bitLength; i++) {
        if (significand.testBit(i)) {
          localHigh |= 1L << (i - 64);
        }
      }

      long biasedExponent = exponent + EXPONENT_OFFSET;

      localHigh |= biasedExponent << 49;

      if (value.signum() == -1 || isNegative) {
        localHigh |= SIGN_BIT_MASK;
      }

      high = localHigh;
      low = localLow;
    }

    private BigDecimal clampAndRound(final BigDecimal initialValue) {
      BigDecimal value;
      if (-initialValue.scale() > MAX_EXPONENT) {
        int diff = -initialValue.scale() - MAX_EXPONENT;
        if (initialValue.unscaledValue().equals(BIG_INT_ZERO)) {
          value = new BigDecimal(initialValue.unscaledValue(), -MAX_EXPONENT);
        } else if (diff + initialValue.precision() > 34) {
          throw new NumberFormatException(
              "Exponent is out of range for Decimal128 encoding of " + initialValue);
        } else {
          BigInteger multiplier = BIG_INT_TEN.pow(diff);
          value = new BigDecimal(initialValue.unscaledValue().multiply(multiplier),
              initialValue.scale() + diff);
        }
      } else if (-initialValue.scale() < MIN_EXPONENT) {
        // Increasing a very negative exponent may require decreasing precision, which is rounding
        // Only round exactly (by removing precision that is all zeroes).  
        // An exception is thrown if the rounding would be inexact:
        int diff = initialValue.scale() + MIN_EXPONENT;
        int undiscardedPrecision = ensureExactRounding(initialValue, diff);
        BigInteger divisor = undiscardedPrecision == 0 ? BIG_INT_ONE : BIG_INT_TEN.pow(diff);
        value = new BigDecimal(initialValue.unscaledValue().divide(divisor),
            initialValue.scale() - diff);
      } else {
        value = initialValue.round(java.math.MathContext.DECIMAL128);
        int extraPrecision = initialValue.precision() - value.precision();
        if (extraPrecision > 0) {
          // only round exactly
          ensureExactRounding(initialValue, extraPrecision);
        }
      }
      return value;
    }

    private int ensureExactRounding(final BigDecimal initialValue, final int extraPrecision) {
      String significand = initialValue.unscaledValue().abs().toString();
      int undiscardedPrecision = Math.max(0, significand.length() - extraPrecision);
      for (int i = undiscardedPrecision; i < significand.length(); i++) {
        if (significand.charAt(i) != '0') {
          throw new NumberFormatException(
              "Conversion to Decimal128 would require inexact rounding of " + initialValue);
        }
      }
      return undiscardedPrecision;
    }

    @Override
    public long getHigh() {
      return high;
    }

    @Override
    public long getLow() {
      return low;
    }



    @Override
    public String toString() {
      if (isNaN()) {
        return "NaN";
      }

      if (isInfinite()) {
        return "Infinity";
      }

      // If the BigDecimal is 0, but the Decimal128 is negative, that means we have -0.
      if (isNegativeZero()) {
        return "Negative zero";
      }

      return getBigDecimal().toString();
    }
  }

}
