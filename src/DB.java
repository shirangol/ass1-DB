import oracle.jdbc.OracleTypes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;

public class DB {
    String userName;
    String password;
    String connectionURL;
    Connection conn;

    //Constructor for create connection to DB (2.a)
    public DB( String connectionURL,  String username, String password){
        this.userName=username;
        this.password=password;
        this.connectionURL=connectionURL;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
//            String connectionURL="jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE";
            this.conn=DriverManager.getConnection(connectionURL,username,password);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //Read a csv file and insert to MediaItems table (2.b)
    public void fileToDataBase(String pathCSV){
        try (BufferedReader br = new BufferedReader(new FileReader(pathCSV))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                insertToMediaItemsTable(values[0],Integer.parseInt(values[1]));
                System.out.println(values[0]+" "+ values[1]);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //Calculate similarity of all pair of items and insert to Similarity table (2.c)
    public void calculateSimilarity(){
        ArrayList<Integer> allMID1= getAllMID();
        ArrayList<Integer> allMID2= getAllMID();
        int maxDistance= getMaxDistance();
        for (int i=0; i<allMID1.size(); i++) {
            for (int j=i+1; j<allMID2.size(); j++) {
                int mid1=allMID1.get(i);
                int mid2=allMID2.get(j);
                float similarity= getSimilarity(mid1,mid2,maxDistance);
                System.out.println("mid1:"+ mid1+"  mid2:"+mid2+ "  sim:"+ similarity);
                insertToSimilarityTable(mid1,mid2,similarity);
            }
        }
    }

    //Print titles of similarity mid (2.d)
    public void printSimilarItems(long mid){
        ArrayList<Long> midSimilarity= getMYMidSimilarity(mid);
        ArrayList<String> titles= getTitle(midSimilarity);

        for (int i=0; i<titles.size();i++){
            System.out.println(titles.get(i));
        }
    }

    //Insert to MediaItems table
    private void insertToMediaItemsTable(String title, int year){
        try {
            PreparedStatement ps= this.conn.prepareStatement("insert into MediaItems (TITLE, PROD_YEAR) values (?,?)");
            ps.setString(1, title);
            ps.setInt(2,year);
            ps.executeUpdate();
            ps.close();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //Get all MID values from MediaItems table
    private ArrayList<Integer> getAllMID(){
        ArrayList<Integer> allMID= new ArrayList<>();
        try {
            PreparedStatement ps= this.conn.prepareStatement("select MID from MediaItems");
            ResultSet rs= ps.executeQuery();
            while(rs.next()){
                allMID.add(rs.getInt("MID"));
            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return allMID;
    }

    //Get the max distance from MaximalDistance function in oracle
    private int getMaxDistance(){
        int maxDis=0;
        try {
            CallableStatement cs= this.conn.prepareCall("{?=call MaximalDistance}");
            cs.registerOutParameter(1, oracle.jdbc.OracleTypes.NUMBER);
            cs.execute();
            maxDis=cs.getInt(1);
            conn.commit();


        } catch (SQLException e) {
            e.printStackTrace();
        }
        return maxDis;
    }

    //Calculate a similarity by SimCalculation function in oracle
    private float getSimilarity(int mid1, int mid2, int maxDistance){

        float similarity=0;
        try {
            CallableStatement cs= this.conn.prepareCall("{?=call SimCalculation(?,?,?)}");
            cs.setInt(2,mid1);
            cs.setInt(3,mid2);
            cs.setInt(4,maxDistance);
            cs.registerOutParameter(1, OracleTypes.FLOAT);
            cs.execute();
            similarity=cs.getFloat(1);
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return similarity;
    }

    //Insert to Similarity table
    private void insertToSimilarityTable(int mid1, int mid2, float similarity){
        try {
            PreparedStatement ps= this.conn.prepareStatement("insert into Similarity (MID1,  MID2, SIMILARITY) values (?,?,?)");
            ps.setInt(1, mid1);
            ps.setInt(2,mid2);
            ps.setFloat(3,similarity);
            ps.executeUpdate();
            ps.clearParameters();
            ps.close();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    //Get all MID for similarity at least 0.3
    private ArrayList<Long> getMYMidSimilarity(long mid) {
        ArrayList<Long> allMID= new ArrayList<>();
        try {
            PreparedStatement ps= this.conn.prepareStatement("select MID1,MID2,SIMILARITY from Similarity where (MID1= ? or MID2=?) and SIMILARITY>=0.3");
            ps.setLong(1,mid);
            ps.setLong(2,mid);
            ResultSet rs= ps.executeQuery();
            while(rs.next()){
                long mid1=rs.getLong("MID1");
                long mid2=rs.getLong("MID2");
                if(mid1==mid){
                    allMID.add(mid2);
                }else {
                    allMID.add(mid1);
                }
            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return allMID;
    }

    //Get titles array of all mid list
    private ArrayList<String> getTitle(ArrayList<Long> mids){
        ArrayList<String> titles= new ArrayList<>();
        try {
            for(long mid: mids){
                PreparedStatement ps= this.conn.prepareStatement("select TITLE from MediaItems where MID= ?");
                ps.setLong(1,mid);
                ResultSet rs= ps.executeQuery();
                while (rs.next()){
                    titles.add(rs.getString("TITLE"));
                }
                rs.close();
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return titles;
    }

}
