import oracle.jdbc.OracleTypes;
import oracle.jdbc.proxy.annotation.Pre;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Assigment {
    String userName;
    String password;
    String connectionURL;
    Connection conn;

    //Constructor for create connection to DB (2.a)
    public Assigment( String connectionURL,  String username, String password){
        this.userName=username;
        this.password=password;
        this.connectionURL=connectionURL;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            this.conn=DriverManager.getConnection(connectionURL,username,password);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //Read a csv file and insert to MediaItems table (2.b)
    public void fileToDataBase(String pathCSV){
        if(conn==null){
            try {
                this.conn=DriverManager.getConnection(connectionURL,userName,password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try (BufferedReader br = new BufferedReader(new FileReader(pathCSV))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                insertToMediaItemsTable(values[0],Integer.parseInt(values[1]));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //Calculate similarity of all pair of items and insert to Similarity table (2.c)
    public void calculateSimilarity(){
        if(conn==null){
            try {
                this.conn=DriverManager.getConnection(connectionURL,userName,password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        ArrayList<Integer> allMID1= getAllMID();
        ArrayList<Integer> allMID2= getAllMID();
        int maxDistance= getMaxDistance();
        for (int i=0; i<allMID1.size(); i++) {
            for (int j=i+1; j<allMID2.size(); j++) {
                int mid1=allMID1.get(i);
                int mid2=allMID2.get(j);
                float similarity= getSimilarity(mid1,mid2,maxDistance);
                insertToSimilarityTable(mid1,mid2,similarity);
            }
        }
    }

    //Print titles of similarity mid (2.d)
    public void printSimilarItems(long mid){
        if(conn==null){
            try {
                this.conn=DriverManager.getConnection(connectionURL,userName,password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        HashMap<Long,Float> midSimilarity= getMYMidSimilarity(mid);
        HashMap<String,Float> titles= getTitle(midSimilarity);
        //sort and print
        titles.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .forEach(item->System.out.println(item.getKey()+ " " + item.getValue()));
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
            cs.setLong(2,mid1);
            cs.setLong(3,mid2);
            cs.setFloat(4,maxDistance);
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
            PreparedStatement ps1= this.conn.prepareStatement("select * from Similarity where (MID1=? and MID2=?) or (MID1= ? and MID2=?)");
            ps1.setLong(1, mid1);
            ps1.setLong(2,mid2);
            ps1.setLong(3, mid2);
            ps1.setLong(4,mid1);
            ResultSet rs= ps1.executeQuery();

            PreparedStatement ps =null;
            if(rs.next()){//checking if MID1+MID2 exists
                ps= this.conn.prepareStatement("update Similarity set SIMILARITY=? where MID1=? and MID2=?");
                ps.setLong(2, mid1);
                ps.setLong(3,mid2);
                ps.setFloat(1,similarity);
                ps.executeUpdate();
                ps.clearParameters();

            }else{
                ps= this.conn.prepareStatement("insert into Similarity (MID1,  MID2, SIMILARITY) values (?,?,?)");
                ps.setLong(1, mid1);
                ps.setLong(2,mid2);
                ps.setFloat(3,similarity);
                ps.executeUpdate();
                ps.clearParameters();

            }
            ps1.close();
            rs.close();
            ps.close();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    //Get all MID for similarity at least 0.3
    private  HashMap<Long,Float> getMYMidSimilarity(long mid) {
        HashMap<Long,Float> allMID= new HashMap<Long, Float>();
        try {
            PreparedStatement ps= this.conn.prepareStatement("select MID1,MID2,SIMILARITY from Similarity where (MID1= ? or MID2=?) and SIMILARITY>=0.3 order by SIMILARITY ASC");
            ps.setLong(1,mid);
            ps.setLong(2,mid);
            ResultSet rs= ps.executeQuery();
            while(rs.next()){
                long mid1=rs.getLong("MID1");
                long mid2=rs.getLong("MID2");
                float sim= rs.getFloat("SIMILARITY");
                if(mid1==mid){
                    allMID.put(mid2,sim);
                }else {
                    allMID.put(mid1,sim);
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
    private HashMap<String,Float> getTitle(HashMap<Long,Float> mids){
        HashMap<String,Float> titles= new HashMap<>();
        try {
            for(Map.Entry mid: mids.entrySet()){
                PreparedStatement ps= this.conn.prepareStatement("select TITLE from MediaItems where MID= ?");
                ps.setLong(1,(Long)(mid.getKey()));
                ResultSet rs= ps.executeQuery();
                while (rs.next()){
                    titles.put(rs.getString("TITLE"),(Float) mid.getValue());
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
