package local.domain1;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Db {
    public java.sql.Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName("org.gjt.mm.mysql.Driver");
        String serverIP = "192.168.255.244";
        //String serverIP = "192.168.1.244";
        String dbName = "rep";
        String userName = "repuser";
        String pass = "letmein";
        return DriverManager.getConnection("jdbc:mysql://"+serverIP+"/"+dbName,userName,pass);
    }
    public int getNumberFromDB(String action,int runid){
        Integer result = 0;
        try {
            java.sql.Connection conn = getConn();
            String query = "";
            if (action.equals("numberOfRecordFields")) {
                query = "select sum(numberOfRecordFields) from rep_record where runid = " + runid;
            }else if (action.equals("numberOfFIleFields")) {
                query = "select sum(numberOfFIleFields) from rep_record where runid = " + runid;
            }else if (action.equals("numberOfMatches")){
                query = "select count(*) from rep_match where runid = " + runid;
            }else if (action.equals("numberOfAllIDs")){
                query = "select count(*) from rep_run order by id";
            }else if (action.equals("numberOfRepsPerName")){
                query = "select count(*) from (select distinct repname from rep_run order by repname)as reps";
            }else if (action.equals("runID")){
                query = "select runId from rep_record order by runId";
            }
            //System.out.println("query: "+query);
            ResultSet rs = conn.prepareStatement(query).executeQuery();

            if (action.startsWith("numberOf")){
                if(rs.next()){
                    result = rs.getInt(1);
                }
            }else if (action.contains("runID")){
                int availid = 1;
                int currentid = 0;
                while (rs.next()) {
                    currentid = rs.getInt(1);
                    if (availid == currentid) {
                        availid++;
                    } else if (availid < currentid) {
                        break;
                    }
                }
                result = availid;
            }
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
