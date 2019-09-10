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
public class MergeJoin {
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

    public void mergeJoin() {
        
        final String DW_QUERY = "SELECT * FROM ucs_satellite_database_7_1_16 ORDER BY norad_number";
        final String PS_QUERY = "SELECT * FROM satcat ORDER BY norad_catalog_number";
        // modify these lines for differnt join argument
        final String joinArgument = "norad_number";
        Map<String, Integer> dbTojoinArgIndex = new HashMap<>();
        dbTojoinArgIndex.put("ucs", 26);
        dbTojoinArgIndex.put("satcat", 4);
        
        final String fileName = "C:/Users/Ching Da Wu/Documents/" +
                                "NetBeansProjects/dataIntegrationLab/" +
                                "build/classes/dataintegrationlab/" + 
                                "output_merge_join_" + joinArgument + ".csv";
        
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
                ) { // execute the query and sort these two databases
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
                    
                    // get the number of rows of two databases
                    psResultSet.last();
                    int dwNumRows = 0, psNumRows = psResultSet.getRow();
                    while (dwResultSet.next()) {
                        dwNumRows++;
                    }
                    
                    while (psResultSet.next()) {
                        psNumRows++;
                    }
                    
                    psResultSet.beforeFirst();
                    
                    ResultSet dwResultSetCopy = dwStatement.executeQuery();
                    ResultSet psResultSetCopy = psStatement.executeQuery();
                                   
                    dwResultSetCopy.next();
                    psResultSetCopy.next();
                    int dwCurrRowId = 1, psCurrRowId = 1;
                    
                    // check if the cursors in both databases is out of bound
                    while (dwCurrRowId <= dwNumRows && psCurrRowId <= psNumRows) {
                                        
                        // get the corresponding value of the join argument in both databases
                        int dwJoinColValue = -1;                       
                        if (dwResultSetCopy.getString(dwJoinColIndex) != null) {
                            dwJoinColValue = Integer.parseInt(dwResultSetCopy.getString(dwJoinColIndex));
                        }
                        int psJoinColValue = Integer.parseInt(psResultSetCopy.getString(psJoinColIndex)); 
                        
                        // if the value of the join argument in ucs database is larger
                        // move toward the cursor in the satcat database by 1
                        // likewise, if the value of the join argument in satcat database is larger
                        // move toward the cursor in the ucs database by 1
                        if (dwJoinColValue > psJoinColValue) {
                            psResultSetCopy.next();
                            psCurrRowId++;
                        } else if (dwJoinColValue < psJoinColValue) {
                            dwResultSetCopy.next();
                            dwCurrRowId++;
                        } else {
                            
                            // if equal, join these two rows
                            for (int i = 1; i <= dwColumnsNumber; i++) {
                                if (i > 1) System.out.print(", ");
                                String columnValue = dwResultSetCopy.getString(i);
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
                                String columnValue = psResultSetCopy.getString(i);
                                if (columnValue == null || columnValue.length() == 0) {
                                    System.out.print("null");
                                } else {
                                    columnValue = columnValue.replaceAll(",", ".");
                                    System.out.print(columnValue);
                                }
                            }
                            System.out.println("");
                                                        
                            // move toward the cursor in both databases by 1
                            dwResultSetCopy.next();
                            psResultSetCopy.next();
                            dwCurrRowId++;
                            psCurrRowId++;
                        }   
                    }                    
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
                
    }    
}
