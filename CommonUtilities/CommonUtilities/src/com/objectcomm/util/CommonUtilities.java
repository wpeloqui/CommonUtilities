package com.objectcomm.util;


import java.util.ArrayList;
import java.util.Iterator;

public class CommonUtilities {

	public static void main(String[] args) {
		
//		PValueTable table = new PValueTable();
//
//		table.generate();

		ArrayList<String> extractFilelist;
		ArrayList<String> historyFilelist;

		final VoterExtractTable extractTable = new VoterExtractTable();
		final VoterHistoryTable historyTable = new VoterHistoryTable();

		VoterFilelist list = new VoterFilelist() ;
		extractFilelist = list.execute("./Resources/VoterExtract");
		historyFilelist = list.execute("./Resources/VoterHistory");

		// Iterate through the voter extract list
		//
		//

		Iterator<String> extract = extractFilelist.iterator();
		while(extract.hasNext()) {
			String name = extract.next().toString();
			System.out.printf("Processing Voter Extract File: %s\n", name);
			extractTable.parseFile(name);
		}

		// Iterate through the voter history list
		//
		//

		Iterator<String> history = historyFilelist.iterator();
		while(history.hasNext()) {
			String name = history.next().toString();
			System.out.printf("Processing Voter History File: %s\n", name);
			historyTable.parseFile(name);
		}

	}

	public static boolean isInteger(String s) {

		try { 
			Integer.parseInt(s); 
		} catch(NumberFormatException e) { 
			return false; 
		}
		// only got here if we didn't return false
		return true;
	}

	public static String fieldCleanup(String s) {

		String a = s.replace("\"", "'");
		String b = a.replace("\\", "/");
		return b;
	}
}