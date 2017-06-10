/**
 *
 * @author randy
 */
package insertmysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.*;

public class insertMySQL
{
    public static void main(String[] args) throws FileNotFoundException, IOException
    {
        /*
        DBase db = new DBase();
        Connection conn = db.connect(
        "jdbc:mysql://localhost:3306/twitter","root","root");
        db.importData(conn,args[0]);*/

        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            String hashtag;

            while (line != null) {
                hashtag = line.split("=")[0];
                System.out.println(hashtag);
            }

        }
    }

}
/*
class DBase
{
    public DBase()
    {
    }

    public Connection connect(String db_connect_str, String db_userid, String db_password)
    {
        Connection conn;
        try
        {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(db_connect_str, db_userid, db_password);

        }
        catch(Exception e)
        {
            e.printStackTrace();
            conn = null;
        }

        return conn;
    }

    public void importData(Connection conn,String filename)
    {
        Statement stmt;
        String query;

        try
        {
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                                        ResultSet.CONCUR_UPDATABLE);

            query = "LOAD DATA INFILE '" + filename + "' INTO TABLE testtable (text,price);";

            stmt.executeUpdate(query);

        }
        catch(Exception e)
        {
            e.printStackTrace();
            stmt = null;
        }
    }
};

*/
