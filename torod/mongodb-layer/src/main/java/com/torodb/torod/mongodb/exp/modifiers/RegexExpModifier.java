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

package com.torodb.torod.mongodb.exp.modifiers;

import com.torodb.torod.core.exceptions.UserToroException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.bson.BsonDocument;

/**
 *
 */
@Immutable
public class RegexExpModifier implements ExpModifier {
    private static final long serialVersionUID = 1L;

    private final boolean i;
    private final boolean m;
    private final boolean x;
    private final boolean s;

    public RegexExpModifier(boolean i, boolean m, boolean x, boolean s) {
        this.i = i;
        this.m = m;
        this.x = x;
        this.s = s;
    }
    
    public RegexExpModifier(String str) {
        boolean _i = false,_m = false,_x = false,_s = false;
        for (char c : str.toCharArray()) {
            switch (c) {
                case 'i': {
                    _i = true;
                    break;
                }
                case 'm': {
                    _m = true;
                    break;
                }
                case 'x': {
                    _x = true;
                    break;
                }
                case 's': {
                    _s = true;
                    break;
                }
                default:
                    throw new UserToroException("unrecognized regex option [" + c + "]");
            }
        }
        i = _i;
        m = _m;
        x = _x;
        s = _s;
    }
    
    static boolean isExpModifier(String key) {
        return key != null && key.equals("$options");
    }
    
    @Nullable
    public static RegexExpModifier fromBSON(BsonDocument exp) {
        for (String key : exp.keySet()) {
            if (!key.equals("$options")) {
                continue;
            }
            if (!exp.get(key).isString()) {
                throw new UserToroException("$options has to be a string");
            }
            return new RegexExpModifier(exp.getString(key).getValue());
        }
        return null;
    }
    
    public int toPatternFlags() {
        int result = 0;
        result |= (this.i ? Pattern.CASE_INSENSITIVE : 0);
        result |= (this.m ? Pattern.MULTILINE : 0);
        result |= (this.x ? Pattern.COMMENTS : 0);
        result |= (this.s ? Pattern.DOTALL : 0);
        return result;
    }

    public boolean isI() {
        return i;
    }

    public boolean isM() {
        return m;
    }

    public boolean isX() {
        return x;
    }

    public boolean isS() {
        return s;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(20);
        sb.append("$options: ");
        if (i) {
            sb.append('i');
        }
        if (m) {
            sb.append('m');
        }
        if (x) {
            sb.append('x');
        }
        if (s) {
            sb.append('s');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + (this.i ? 1 : 0);
        hash = 19 * hash + (this.m ? 1 : 0);
        hash = 19 * hash + (this.x ? 1 : 0);
        hash = 19 * hash + (this.s ? 1 : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RegexExpModifier other = (RegexExpModifier) obj;
        if (this.i != other.i) {
            return false;
        }
        if (this.m != other.m) {
            return false;
        }
        if (this.x != other.x) {
            return false;
        }
        if (this.s != other.s) {
            return false;
        }
        return true;
    }
    

}
