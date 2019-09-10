/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dataintegrationlab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.stream.Stream; 
import java.nio.charset.StandardCharsets; 
import java.nio.file.*; 
import java.io.*;
import java.text.*;
/**
 *
 * @author Ching Da Wu
 */

class Tuple {
    private final int launch_year;
    private final int launch_of_the_year;
    private final String piece_of_the_launch;
    private final int norad_catalog_number;
    private final String multiple_name_flag;
    private final String payload_flag;
    private final String operational_status_code;
    private final String satellite_name;
    private final String source_or_ownership;
    private final java.sql.Date launch_date;
    private final String launch_site;
    private final java.sql.Date decay_date;
    private final double orbital_period;
    private final double inclination;
    private final int apogee_altitude;
    private final int perigee_altitude;
    private final double radar_cross_section;
    private final String orbital_status_code;
    public Tuple(int launch_year, int launch_of_the_year, String piece_of_the_launch,
                 int norad_catalog_number, String multiple_name_flag, String payload_flag,
                 String operational_status_code, String satellite_name, String source_or_ownership,
                 java.sql.Date launch_date, String launch_site, java.sql.Date decay_date, 
                 double orbital_period, double inclination, int apogee_altitude, 
                 int perigee_altitude, double radar_cross_section, String orbital_status_code) {
        
        this.launch_year = launch_year;
        this.launch_of_the_year = launch_of_the_year;
        this.piece_of_the_launch = piece_of_the_launch;
        this.norad_catalog_number = norad_catalog_number;
        this.multiple_name_flag = multiple_name_flag;
        this.payload_flag = payload_flag;
        this.operational_status_code = operational_status_code;
        this.satellite_name = satellite_name;
        this.source_or_ownership = source_or_ownership;
        this.launch_date = launch_date;
        this.launch_site = launch_site;
        this.decay_date = decay_date;
        this.orbital_period = orbital_period;
        this.inclination = inclination;
        this.apogee_altitude = apogee_altitude;
        this.perigee_altitude = perigee_altitude;
        this.radar_cross_section = radar_cross_section;
        this.orbital_status_code = orbital_status_code;   
    }
    
    public int getLaunch_year() {
        return launch_year;
    }

    public int getLaunch_of_the_year() {
        return launch_of_the_year;
    }

    public String getPiece_of_the_launch() {
        return piece_of_the_launch;
    }

    public int getNorad_catalog_number() {
        return norad_catalog_number;
    }

    public String getMultiple_name_flag() {
        return multiple_name_flag;
    }

    public String getPayload_flag() {
        return payload_flag;
    }

    public String getOperational_status_code() {
        return operational_status_code;
    }

    public String getSatellite_name() {
        return satellite_name;
    }

    public String getSource_or_ownership() {
        return source_or_ownership;
    }

    public java.sql.Date getLaunch_date() {
        return launch_date;
    }

    public String getLaunch_site() {
        return launch_site;
    }

    public java.sql.Date getDecay_date() {
        return decay_date;
    }

    public double getOrbital_period() {
        return orbital_period;
    }

    public double getInclination() {
        return inclination;
    }

    public int getApogee_altitude() {
        return apogee_altitude;
    }

    public int getPerigee_altitude() {
        return perigee_altitude;
    }

    public double getRadar_cross_section() {
        return radar_cross_section;
    }

    public String getOrbital_status_code() {
        return orbital_status_code;
    }
}

public class SatcatBuilder {

    private final String url = "jdbc:postgresql://localhost/postgres";
    private final String user = "postgres";
    private final String password = "1234567890";

    private final double MISSING_DOUBLE = -1.0;
    private final int MISSING_INTEGER = -1;
    private final java.sql.Date MISSING_DATE;
    
    public SatcatBuilder() {
        Calendar defaultDate = Calendar.getInstance();
        defaultDate.set(1900, 0, 1);
        MISSING_DATE = new java.sql.Date(defaultDate.getTime().getTime());
    }
    
    public Connection connect() {
        
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
 
        return conn;
    }

    public static List<String> readFileInList(String fileName) { 

        List<String> lines = Collections.emptyList();
        try { 
            lines = 
            Files.readAllLines(Paths.get(fileName)); 
        } catch (IOException e) {  
            e.printStackTrace(); 
        } 
        return lines;
     
    }
        
    public List<Tuple> generateTuples(List<String> linesList) {
                
        List<Tuple> tuplesList = new ArrayList<>();
        Iterator<String> itr = linesList.iterator();
        while (itr.hasNext()) {
            String line = itr.next();
            int launch_year = Integer.parseInt(line.substring(0, 4).trim());
            int launch_of_the_year = Integer.parseInt(line.substring(5, 8).trim());
            String piece_of_the_launch = line.substring(8, 11).trim();
            int norad_catalog_number = Integer.parseInt(line.substring(13, 18).trim());
            String multiple_name_flag = line.substring(19, 20).trim();
            String payload_flag = line.substring(20, 21).trim();
            String operational_status_code = line.substring(21, 22).trim();;
            String satellite_name = line.substring(23, 47).trim();
            String source_or_ownership = line.substring(49, 54).trim();
            String launch_site = line.substring(68, 73).trim();
            
            String tmpStr = line.substring(87, 94).trim();
            double orbital_period = MISSING_DOUBLE;
            if (tmpStr.length() != 0) {
                orbital_period = Double.parseDouble(tmpStr);
            }
            
            tmpStr = line.substring(96, 101).trim();
            double inclination = MISSING_DOUBLE;
            if (tmpStr.length() != 0) {
                orbital_period = Double.parseDouble(tmpStr);
            }
            
            tmpStr = line.substring(103, 109).trim();
            int apogee_altitude = MISSING_INTEGER;
            if (tmpStr.length() != 0) {
                apogee_altitude = Integer.parseInt(tmpStr);
            }
            
            tmpStr = line.substring(111, 117).trim();
            int perigee_altitude = MISSING_INTEGER;
            if (tmpStr.length() != 0) {
                perigee_altitude = Integer.parseInt(tmpStr);
            }
            
            tmpStr = line.substring(119, 127).trim();
            double radar_cross_section = MISSING_DOUBLE;
            if (!tmpStr.equals("N/A")) {
                radar_cross_section = Double.parseDouble(tmpStr);
            }
            
            String orbital_status_code = line.substring(129, 132).trim();
            java.sql.Date launch_date = MISSING_DATE; 
            java.sql.Date decay_date = MISSING_DATE;
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-mm-dd");            
            try {
                java.util.Date date = formatter.parse(line.substring(56, 66).trim());
                launch_date = new java.sql.Date(date.getTime());
                tmpStr = line.substring(75, 85).trim();
                if (tmpStr.length() != 0) {
                    date = formatter.parse(tmpStr);
                    decay_date = new java.sql.Date(date.getTime());
                }
            } catch (ParseException ex) {
                System.out.println(ex.getMessage());
            }
            
            Tuple tuple = new Tuple(launch_year, launch_of_the_year, piece_of_the_launch, 
                                    norad_catalog_number, multiple_name_flag, payload_flag,
                                    operational_status_code, satellite_name, source_or_ownership,
                                    launch_date, launch_site, decay_date, orbital_period,
                                    inclination, apogee_altitude, perigee_altitude, 
                                    radar_cross_section, orbital_status_code);
            tuplesList.add(tuple);
        }
        return tuplesList;
    }
    
    public void insertTuples(List<Tuple> tuplesList) {
                
        String SQL = "insert into satcat(launch_year, launch_of_the_year, "
                + "piece_of_the_launch, norad_catalog_number, multiple_name_flag, payload_flag, "
                + "operational_status_code, satellite_name, source_or_ownership, "
                + "launch_date, launch_site, decay_date, orbital_period, inclination, "
                + "apogee_altitude, perigee_altitude, radar_cross_section, orbital_status_code)"
                + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        
        try (
                Connection conn = connect();
                PreparedStatement statement = conn.prepareStatement(SQL);) {
            int count = 0;
 
            for (Tuple tuple : tuplesList) {
                statement.setInt(1, tuple.getLaunch_year());
                statement.setInt(2, tuple.getLaunch_of_the_year());
                statement.setString(3, tuple.getPiece_of_the_launch());
                statement.setInt(4, tuple.getNorad_catalog_number());
                statement.setString(5, tuple.getMultiple_name_flag());
                statement.setString(6, tuple.getPayload_flag());
                statement.setString(7, tuple.getOperational_status_code());
                statement.setString(8, tuple.getSatellite_name());
                statement.setString(9, tuple.getSource_or_ownership());
                statement.setDate(10, tuple.getLaunch_date());
                statement.setString(11, tuple.getLaunch_site());
                
                java.sql.Date decay_date = tuple.getDecay_date();
                if (decay_date.equals(MISSING_DATE)) {
                    statement.setNull(12, java.sql.Types.DATE);
                } else {
                    statement.setDate(12, decay_date);
                }
                                
                double orbital_period = tuple.getOrbital_period();
                if (orbital_period == MISSING_DOUBLE) {
                    statement.setNull(13, java.sql.Types.NUMERIC);
                } else {
                    statement.setDouble(13, orbital_period);
                }
                
                double inclination = tuple.getInclination();
                if (inclination == MISSING_DOUBLE) {
                    statement.setNull(14, java.sql.Types.NUMERIC);
                } else {
                    statement.setDouble(14, inclination);
                }
                
                int apogee_altitude = tuple.getApogee_altitude();
                if (apogee_altitude == MISSING_INTEGER) {
                    statement.setNull(15, java.sql.Types.INTEGER);
                } else {
                    statement.setInt(15, apogee_altitude);
                }
                
                int perigee_altitude = tuple.getPerigee_altitude();
                if (perigee_altitude == MISSING_INTEGER) {
                    statement.setNull(16, java.sql.Types.INTEGER);
                } else {
                    statement.setInt(16, perigee_altitude);
                }
                
                double radar_cross_section = tuple.getRadar_cross_section();
                if (radar_cross_section == MISSING_DOUBLE) {
                    statement.setNull(17, java.sql.Types.NUMERIC);
                } else {
                    statement.setDouble(17, radar_cross_section);
                }
                                
                statement.setString(18, tuple.getOrbital_status_code());
                statement.addBatch();
                count++;
                // execute every 100 rows or less
                if (count % 100 == 0 || count == tuplesList.size()) {
                    statement.executeBatch();
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
    
    public void buildSatcatDB() {
        
        SatcatBuilder sb = new SatcatBuilder();
        final String SQL = "create table if not exists satcat(launch_year integer, "
                + "launch_of_the_year integer, piece_of_the_launch varchar(3), "
                + "norad_catalog_number integer PRIMARY KEY, multiple_name_flag varchar(1), "
                + "payload_flag varchar(1), operational_status_code varchar(1), "
                + "satellite_name varchar(24), source_or_ownership varchar(5), "
                + "launch_date date, launch_site varchar(5), decay_date date, "
                + "orbital_period numeric, inclination numeric, apogee_altitude integer, "
                + "perigee_altitude integer, radar_cross_section numeric, "
                + "orbital_status_code varchar(3))";        
        try {
            Connection conn = sb.connect();
            PreparedStatement statement = conn.prepareStatement(SQL);
            statement.execute();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
         
        final String fileName = "C:/Users/Ching Da Wu/Documents/" +
                                "NetBeansProjects/dataIntegrationLab/" +
                                "build/classes/dataintegrationlab/satcat.txt";
        
        List<String> linesList = readFileInList(fileName);
        List<Tuple> tuplesList = sb.generateTuples(linesList);
        sb.insertTuples(tuplesList);
    }  
}

