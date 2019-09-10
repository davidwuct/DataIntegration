/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dataintegrationlab;

public class DataIntegrationLab {
    
    public static void main(String[] args) {
                        
        // to run the program successfully, the jar files below are needed
        // dw-jdbc-0.4.4.jar, jackson-core-2.9.6.jar, postgresql-42.2.5.jar

        // build a local PostgreSQL database and load it with the SATCAT data
        SatcatBuilder sb = new SatcatBuilder();
        sb.buildSatcatDB();
        
        // implement nested loop join algorithm
        NestedLoopJoin nlj = new NestedLoopJoin();
        nlj.nestedLoopJoin();
        
        // implement merge join algorithm
        MergeJoin mj = new MergeJoin();
        mj.mergeJoin();

    }
}
