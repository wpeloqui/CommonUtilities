package com.objectcomm.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Types;

public class VoterHistoryTable {

	public void parseFile(String csvFile) {

		Connection connection = null;
		PreparedStatement preparedStatement = null;

		String sql = "INSERT INTO voter_database.voter_history (" +
				"county_code, " +
				"voter_id, " +
				"election_date, " +
				"election_type, " +
				"election_code) " +
				"values (?, ?, ?, ?, ?)";

		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\t";

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

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				String electionDate = null;

				int voterId = 0;

				// use tab as separator
				String[] fields = line.split(cvsSplitBy);

				try {
					if (CommonUtilities.isInteger(fields[1])) {
						voterId = Integer.parseInt(fields[1]);
					}

					if (fields[2].length() == 10) {

						electionDate = fields[2].substring(6) + "-" +
								fields[2].substring(0, 2) + "-" +
								fields[2].substring(3, 5);
					}

					preparedStatement.setString(1, fields[0]);
					preparedStatement.setInt(2, voterId);

					if (electionDate != null)
						preparedStatement.setDate(3, java.sql.Date.valueOf(electionDate));
					else
						preparedStatement.setNull(3, Types.NULL);

					preparedStatement.setString(4, fields[3]);
					preparedStatement.setString(5, fields[4]);

					preparedStatement.execute();

				}
				catch (SQLTimeoutException e) {
					System.out.printf("ERROR: SQLTimeoutException: %s\n%s\n", preparedStatement.toString() + ": ", e.getMessage());
				}
				catch (SQLException e) {
					System.out.printf("ERROR: SQLException: %s\n%s\n", preparedStatement.toString() + ": ", e.getMessage());
				}
				catch (Exception e) {
					System.out.printf("ERROR: Exception: %s\n%s\n", preparedStatement.toString() + ": ", e.getMessage());
				}			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		try { preparedStatement.executeBatch(); }  catch (SQLException e) { e.printStackTrace(); } // insert remaining records
		try { if (preparedStatement != null) preparedStatement.close(); } catch (SQLException e) { e.printStackTrace(); }
		try { if (connection != null) connection.close(); } catch (SQLException e) { e.printStackTrace(); }
	}
}