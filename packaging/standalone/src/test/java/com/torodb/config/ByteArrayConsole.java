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

package com.torodb.config;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.beust.jcommander.internal.Console;

public class ByteArrayConsole implements Console {
	
	private final ByteArrayOutputStream byteArrayOutputStream;
	private final PrintStream printStream;
	
	public ByteArrayConsole() {
		byteArrayOutputStream = new ByteArrayOutputStream();
		printStream = new PrintStream(byteArrayOutputStream);
	}
	
	public ByteArrayOutputStream getByteArrayOutputStream() {
		return byteArrayOutputStream;
	}

	@Override
	public char[] readPassword(boolean echoInput) {
		return null;
	}
	
	@Override
	public void println(String msg) {
		printStream.println(msg);
	}
	
	@Override
	public void print(String msg) {
		printStream.print(msg);
	}
}
