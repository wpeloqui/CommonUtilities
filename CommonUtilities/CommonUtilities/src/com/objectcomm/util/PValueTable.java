package com.objectcomm.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.HashMap;
import java.util.Map;

public class PValueTable {

	public void generate() {

		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet results = null;
		Map<Integer, Integer> p = new HashMap<Integer, Integer>();

		String sql = "SELECT voter_history.voter_id, voter_history.election_date, " +
				"voter_history.election_code FROM voter_history " +
				"join voter_extract ON voter_history.voter_id = voter_extract.voter_id " +
				"where voter_extract.party_affiliation = 'REP' and voter_history.election_type = 'PRI' " +
				"ORDER BY voter_history.voter_id, voter_history.election_date DESC;";

		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();

			String connectionUrl = "jdbc:mysql://localhost:3306/voter_database";
			String connectionUser = "root";
			String connectionPassword = "root";

			connection = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);

			preparedStatement = connection.prepareStatement(sql);

		} catch (Exception e) {
			e.printStackTrace();
		}

		try {

			results = preparedStatement.executeQuery();

			int last_voter_id = 0;
			int value = 0;
			int total_value = 0;
			int election_count = 0;
			Integer count = new Integer(0);

			while (results.next()) {
				
				int voter_id = results.getInt("voter_id");
				String election_code = results.getString("election_code");
				//Date election_date = results.getDate("election_date");

				if (last_voter_id != voter_id) {
					
					// This is not the first record so print it out
					//
					//
					
					if (last_voter_id != 0) {
						
						// give some points if there have not been five elections
						//
						//
						
						if (election_count < 5) {
							total_value = total_value + 10;
							//System.out.println(voter_id + "\tBonus\t\t\t" + 10);
						}
						
						//System.out.println("Voter Total Value\t" + last_voter_id + "\tP" + total_value);
						//System.out.println(last_voter_id + ", P" + total_value);
						
						count = (Integer) p.get(total_value);
						
						if (count == null)
							p.put(total_value, 1);
						else
							p.put(total_value, count+1);
					}
					
					last_voter_id = voter_id;
					value = 30;
					total_value = 0;
					election_count = 0;
				}

				if (election_code != "N") {
					//System.out.println(voter_id + "\t" + election_date + "\t" + election_code + "\t" + value);
					total_value = total_value + value;
				} else {
					//System.out.println(voter_id + "\t" + election_date + "\t" + election_code);
				}

				if (value > 10) {
					value = value - 5;
				} else if (value == 10) {
					value = 0;
				}
				
				election_count ++;
			}
			
			// give some points if there have not been five elections
			//
			//
			
			if (election_count < 5) {
				total_value = total_value + 10;
			}
			
			//System.out.println("Voter Total Value\t" + last_voter_id + "\tP" + total_value);
			//System.out.println(last_voter_id + ", P" + total_value);
			
			count = (Integer) p.get(total_value);
			
			if (count == null)
				p.put(total_value, 1);
			else
				p.put(total_value, count+1);
		}
		catch (SQLTimeoutException e) {
			System.out.printf("ERROR: SQLTimeoutException: %s\n%s\n", preparedStatement.toString() + ": ", e.getMessage());
		}
		catch (SQLException e) {
			System.out.printf("ERROR: SQLException: %s\n%s\n", preparedStatement.toString() + ": ", e.getMessage());
		}
		catch (Exception e) {
			System.out.printf("ERROR: Exception: %s\n%s\n", preparedStatement.toString() + ": ", e.getMessage());
		}


		try { preparedStatement.executeBatch(); }  catch (SQLException e) { e.printStackTrace(); } // insert remaining records
		try { if (preparedStatement != null) preparedStatement.close(); } catch (SQLException e) { e.printStackTrace(); }
		try { if (connection != null) connection.close(); } catch (SQLException e) { e.printStackTrace(); }
		
		for (Map.Entry<Integer, Integer> entry : p.entrySet()) {
			System.out.println(entry.getKey() + ", " + entry.getValue());
		}
	}
}