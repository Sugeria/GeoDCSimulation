/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

/**
 * The Log class used for performing loggin of the simulation process. It provides the ability to
 * substitute the output stream by any OutputStream subclass.
 * 
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 2.0
 */
public class Log {

	/** The Constant LINE_SEPARATOR. */
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");

	/** The output. */
	private static OutputStream output;

	/** The disable output flag. */
	private static boolean disabled;
	
	public static String file_name = "";

	/**
	 * Prints the message.
	 * 
	 * @param message the message
	 */
	public static void print(String message) {
		if (!isDisabled()) {
			message += "\n\n";
			try {
				getOutput().write(message.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
			AddtoFile(message);
		}
	}

	public static void singleprint(String message) {
		if (!isDisabled()) {
			try {
				getOutput().write(message.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
			singleAddtoFile(message);
		}
	}
	
	/**
	 * Prints the message passed as a non-String object.
	 * 
	 * @param message the message
	 */
	public static void print(Object message) {
		if (!isDisabled()) {
			print(String.valueOf(message));
		}
	}

	/**
	 * Prints the line.
	 * 
	 * @param message the message
	 */
	public static void printLine(String message) {
		if (!isDisabled()) {
			print(message + LINE_SEPARATOR);
		}
	}
	
	public static void singleprintLine(String message) {
		if (!isDisabled()) {
			singleprint(message + LINE_SEPARATOR);
		}
	}

	/**
	 * Prints the empty line.
	 */
	public static void printLine() {
		if (!isDisabled()) {
			print(LINE_SEPARATOR);
		}
	}

	/**
	 * Prints the line passed as a non-String object.
	 * 
	 * @param message the message
	 */
	public static void printLine(Object message) {
		if (!isDisabled()) {
			printLine(String.valueOf(message));
		}
	}

	/**
	 * Prints a string formated as in String.format().
	 * 
	 * @param format the format
	 * @param args the args
	 */
	public static void format(String format, Object... args) {
		if (!isDisabled()) {
			print(String.format(format, args));
		}
	}

	/**
	 * Prints a line formated as in String.format().
	 * 
	 * @param format the format
	 * @param args the args
	 */
	public static void formatLine(String format, Object... args) {
		if (!isDisabled()) {
			printLine(String.format(format, args));
		}
	}

	/**
	 * Sets the output.
	 * 
	 * @param _output the new output
	 */
	public static void setOutput(OutputStream _output) {
		output = _output;
	}

	/**
	 * Gets the output.
	 * 
	 * @return the output
	 */
	public static OutputStream getOutput() {
		if (output == null) {
			setOutput(System.out);
		}
		return output;
	}

	/**
	 * Sets the disable output flag.
	 * 
	 * @param _disabled the new disabled
	 */
	public static void setDisabled(boolean _disabled) {
		disabled = _disabled;
	}

	/**
	 * Checks if the output is disabled.
	 * 
	 * @return true, if is disable
	 */
	public static boolean isDisabled() {
		return disabled;
	}

	/**
	 * Disables the output.
	 */
	public static void disable() {
		setDisabled(true);
	}

	/**
	 * Enables the output.
	 */
	public static void enable() {
		setDisabled(false);
	}
	
	public static void singleAddtoFile(String msg) {
		try {
			java.util.Date d = new java.util.Date();
			if (file_name == "") {
				file_name = "c:/log/cloudSim_Log" + d.getTime() + ".txt";
			}
			File file = new File(file_name);
			// if file doesnt exists, then create it
			if (!file.exists()) {

				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			String text = msg;
			fw.write(text);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void AddtoFile(String msg) {
		try {
			java.util.Date d = new java.util.Date();
			if (file_name == "") {
				file_name = "c:/log/cloudSim_Log" + d.getTime() + ".txt";
			}
			File file = new File(file_name);
			// if file doesnt exists, then create it
			if (!file.exists()) {

				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			String text = System.lineSeparator()
					+ msg.replace("\n", System.lineSeparator());
			fw.write(text);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
