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

public class VoterExtractTable {



	public void parseFile(String csvFile) {

		Connection connection = null;
		PreparedStatement preparedStatement = null;

		String sql = "INSERT INTO voter_database.voter_extract (" +
				"county_code, " +
				"voter_id, " +
				"name_last, " +
				"name_suffix, " +
				"name_first, " +
				"name_middle, " +
				"suppress_contact_info, " +
				"residence_address_line_1, " +
				"residence_address_line_2, " +
				"residence_city, " +
				"residence_state, " +
				"residence_zipcode, " +
				"mailing_address_line_1, " +
				"mailing_address_line_2, " +
				"mailing_address_line_3, " +
				"mailing_city, " +
				"mailing_state, " +
				"mailing_zipcode, " +
				"mailing_country, " +
				"gender, " +
				"race, " +
				"birth_date, " +
				"registration_date, " +
				"party_affiliation, " +
				"precinct, " +
				"precinct_group, " +
				"precinct_split, " +
				"precinct_suffix, " +
				"voter_status, " +
				"congressional_district, " +
				"house_district, " +
				"senate_district, " +
				"county_commission_district, " +
				"school_board_district, " +
				"daytime_area_code, " +
				"daytime_phone_number, " +
				"email_address) " +
				"values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + 
				"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
				"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
				"?, ?, ?, ?, ?, ?, ?)";

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

				String emailAddress = "";
				String birthDate = null;
				String registrationDate = null;

				int phoneNumber = 0;
				int areaCode = 0;
				int voterId = 0;

				// use tab as separator
				String[] fields = line.split(cvsSplitBy);

				try {
					if (CommonUtilities.isInteger(fields[1])) {
						voterId = Integer.parseInt(fields[1]);
					}

					if (fields.length >= 38) {
						emailAddress = fields[37];
					}

					if (fields.length >= 36) {
						if (CommonUtilities.isInteger(fields[35])) {
							phoneNumber = Integer.parseInt(fields[35]);
						}
					}

					if (fields.length >= 35) {
						if (CommonUtilities.isInteger(fields[34])) {
							areaCode = Integer.parseInt(fields[34]);
						}
					}

					if (fields[21].length() == 10) {

						birthDate = fields[21].substring(6) + "-" +
								fields[21].substring(0, 2) + "-" +
								fields[21].substring(3, 5);
					}
					/*					else
						System.out.printf("WARNING: Voter Id = %d birthdate = %s (length = %d)\n",
								voterId, fields[21], fields[21].length());
					 */
					if (fields[22].length() == 10) {

						registrationDate = fields[22].substring(6) + "-" +
								fields[22].substring(0, 2) + "-" +
								fields[22].substring(3, 5);
					}
					/*					else
						System.out.printf("WARNING: Voter Id = %d registrationDate = %s (length = %d)\n",
								voterId, fields[22], fields[22].length());
					 */
					preparedStatement.setString(1, fields[0]);
					preparedStatement.setInt(2, voterId);

					for (int i=3; i<22; i++) {
						if ((fields.length >= i) && (fields[i-1].length() > 0)) {
							preparedStatement.setString(i, fields[i-1]);
						}
						else {
							preparedStatement.setNull(i, Types.NULL);
						}
					}

					if (birthDate != null)
						preparedStatement.setDate(22, java.sql.Date.valueOf(birthDate));
					else
						preparedStatement.setNull(22, Types.NULL);

					if (registrationDate != null)
						preparedStatement.setDate(23, java.sql.Date.valueOf(registrationDate));
					else
						preparedStatement.setNull(23, Types.NULL);

					for (int i=24; i<35; i++) {
						if ((fields.length >= i) && (fields[i-1].length() > 0)) {
							preparedStatement.setString(i, fields[i-1]);
						}
						else {
							preparedStatement.setNull(i, Types.NULL);
						}
					}

					preparedStatement.setInt(35, areaCode);
					preparedStatement.setInt(36, phoneNumber);
					preparedStatement.setString(37, emailAddress);

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
				}
			}

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