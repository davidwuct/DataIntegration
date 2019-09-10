/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dataintegrationlab;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 *
 * @author Ching Da Wu
 */
public class NestedLoopJoin {
    
    private final String dwUrl = "jdbc:data:world:sql:phalseid:satellite-data";
    private final String dwUser = "davidwu";
    private final String dwApiToken = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50OmRhdm"
                + "lkd3UiLCJpc3MiOiJhZ2VudDpkYXZpZHd1OjpjNWJiNjZiMy04ZmM4LTRjNWEtOGMzMS02YjcwN"
                + "WM2MWFkN2UiLCJpYXQiOjE1NTYyMTQzMjcsInJvbGUiOlsidXNlcl9hcGlfYWRtaW4iLCJ1c2Vy"
                + "X2FwaV9yZWFkIiwidXNlcl9hcGlfd3JpdGUiXSwiZ2VuZXJhbC1wdXJwb3NlIjp0cnVlLCJzYW1"
                + "sIjp7fX0.Ybb6gHlDAPh2I9OceYcV3dkFAGiuEyCoPz_WavIZ2wg2gdoDDttcrcVAfEnH4cGe2o"
                + "DaoSBeYn2VyPKm4EDGLw";
    
    private final String psUrl = "jdbc:postgresql://localhost/postgres";
    private final String psUser = "postgres";
    private final String psPassword = "1234567890";
    
    public void nestedLoopJoin() {
    
        final String DW_QUERY = "SELECT * FROM ucs_satellite_database_7_1_16";
        final String PS_QUERY = "SELECT * FROM satcat";
        
        // for the join argument "norad_number", use these four lines below
        /*final String joinArgument = "norad_number";
        Map<String, Integer> dbTojoinArgIndex = new HashMap<>();
        dbTojoinArgIndex.put("ucs", 26);
        dbTojoinArgIndex.put("satcat", 4);*/
        
        // for the join argument "satellite_name", use these four lines below
        final String joinArgument = "satellite_name";
        Map<String, Integer> dbTojoinArgIndex = new HashMap<>();
        dbTojoinArgIndex.put("ucs", 1);
        dbTojoinArgIndex.put("satcat", 8);
        
        final String fileName = "C:/Users/Ching Da Wu/Documents/" +
                                "NetBeansProjects/dataIntegrationLab/" +
                                "build/classes/dataintegrationlab/" + 
                                "output_nested_loop_join_" + joinArgument + ".csv";
        
        // write results into the csv file
        try {
            PrintStream o = new PrintStream(new File(fileName));
            System.setOut(o);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
           
        try {
            try (
                final Connection dwConnection = DriverManager.getConnection(dwUrl, dwUser, dwApiToken);
                final Connection psConnection = DriverManager.getConnection(psUrl, psUser, psPassword);
                // get a connection to the database, which will automatically be closed when done
                final PreparedStatement dwStatement = dwConnection.prepareStatement(DW_QUERY);
                final PreparedStatement psStatement = psConnection.prepareStatement(PS_QUERY, 
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ) {
                try (
                    ResultSet dwResultSet = dwStatement.executeQuery();
                    ResultSet psResultSet = psStatement.executeQuery();
                ) { // execute the query
                    ResultSetMetaData dwRsmd = dwResultSet.getMetaData();
                    ResultSetMetaData psRsmd = psResultSet.getMetaData();
                    
                    // get the index of join argument in two databases
                    int dwJoinColIndex = dbTojoinArgIndex.get("ucs");
                    int psJoinColIndex = dbTojoinArgIndex.get("satcat");
                    // print the name of the columns in two databases
                    int dwColumnsNumber = dwRsmd.getColumnCount();
                    for (int i = 1; i <= dwColumnsNumber; i++) {
                        if (i > 1) System.out.print(", ");
                        System.out.print(dwRsmd.getColumnName(i));
                    }
                    
                    System.out.print(", ");    
                    int psColumnsNumber = psRsmd.getColumnCount();
                    for (int i = 1; i <= psColumnsNumber; i++) {
                        if (i == psJoinColIndex) {
                            continue;
                        }
                        if (i > 1) System.out.print(", ");
                        System.out.print(psRsmd.getColumnName(i));
                    }
                    System.out.println("");
                    
                    // pick a row of ucs database, , and get its corresponding value of the join argument
                    while (dwResultSet.next()) {
                        
                        // if join argument is "norad_number", use these four line below
                        /*int dwJoinColValue = -1;
                        if (dwResultSet.getString(dwJoinColIndex) != null) {
                            dwJoinColValue = Integer.parseInt(dwResultSet.getString(dwJoinColIndex));
                        }*/
                        
                        String dwJoinColValue = "";
                        if (dwResultSet.getString(dwJoinColIndex) != null) {
                            dwJoinColValue = dwResultSet.getString(dwJoinColIndex);
                        }
                        
                        // pick a row of satcat database, and get its corresponding value of the join argument
                        while (psResultSet.next()) {
                            
                            // if join argument is "norad_number", use the line below
                            /*int psJoinColValue = Integer.parseInt(psResultSet.getString(psJoinColIndex));*/
                            String psJoinColValue = psResultSet.getString(psJoinColIndex);
                            
                            // compare two values, if equal, join these two rows
                            // if join argument is "norad_number", use these three line below
                            /*if (dwJoinColValue != psJoinColValue) {
                                continue;
                            }*/
                            
                            // compare two values, if equal, join these two rows
                            // simple equality predicate
                            if (!dwJoinColValue.equals(psJoinColValue)) {
                                continue;
                            }
                            
                            // smarter equality predicate
                            /*if (dwJoinColValue.length() < psJoinColValue.length()) {
                                if (!psJoinColValue.contains(dwJoinColValue)) {
                                    continue;
                                }
                            } else {
                                if (!dwJoinColValue.contains(psJoinColValue)) {
                                    continue;
                                }                                
                            }*/
                            
                            // print the values of these two rows
                            for (int i = 1; i <= dwColumnsNumber; i++) {
                                if (i > 1) System.out.print(", ");
                                String columnValue = dwResultSet.getString(i);
                                if (columnValue == null || columnValue.length() == 0) {
                                    System.out.print("null");
                                } else {
                                    columnValue = columnValue.replaceAll(",", ".");
                                    System.out.print(columnValue);
                                }
                            }                            
                            
                            System.out.print(", ");
                            for (int i = 1; i <= psColumnsNumber; i++) {
                                if (i == psJoinColIndex) {
                                    continue;
                                }
                                if (i > 1) System.out.print(", ");
                                String columnValue = psResultSet.getString(i);
                                if (columnValue == null || columnValue.length() == 0) {
                                    System.out.print("null");
                                } else {
                                    columnValue = columnValue.replaceAll(",", ".");
                                    System.out.print(columnValue);
                                }
                            }
                            System.out.println("");
                        }
                        // move the cursor of the satcat database to the beginning
                        psResultSet.beforeFirst();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    
    }
}
