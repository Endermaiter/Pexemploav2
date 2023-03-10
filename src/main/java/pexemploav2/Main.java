
package pexemploav2;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import java.sql.*;
import java.util.Iterator;
import java.util.List;

public class Main {

    static MongoClient mongoClient = MongoClients.create();
    static MongoDatabase database = mongoClient.getDatabase("test");
    static MongoCollection<Document> collection = database.getCollection("empretodos");

    public static Connection conexion() throws SQLException {
        String driver = "jdbc:postgresql:";
        String host = "//localhost:";
        String porto = "5432";
        String sid = "postgres";
        String usuario = "dam2a";
        String password = "castelao";
        String url = driver + host + porto + "/" + sid;
        Connection conn = DriverManager.getConnection(url, usuario, password);
        return conn;
    }

    public static void main(String[] args) throws SQLException {

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("horasextratodos.odb");
        EntityManager em = emf.createEntityManager();

        //Borrar tabla postgres finalinf

        PreparedStatement ps;
        ps = conexion().prepareStatement("DELETE FROM finalinf");
        ps.executeUpdate();


        Statement instrucciones = conexion().createStatement();
        ResultSet rs;
        rs = instrucciones.executeQuery("select informaticos.*, (fillos).homes, (fillos).mulleres from informaticos");

        while (rs.next()) {
            int cinf = rs.getInt("cinf");
            String dniinf = rs.getString("dniinf");
            int fillosHomes = rs.getInt("homes");
            int fillosMulleres = rs.getInt("mulleres");

            int totalFillos = fillosHomes + fillosMulleres;

            if (totalFillos > 0) {
                BasicDBObject condicion = new BasicDBObject();
                FindIterable<Document> iterDoc;
                Iterator<Document> iterator;
                condicion.put("dnie", dniinf);
                iterDoc = collection.find(condicion);
                iterator = iterDoc.iterator();

                int sb = (int) iterator.next().get("sb");

                Iterator<Document> iterator2;
                FindIterable<Document> iterDoc2 = collection.find(condicion);
                iterator2 = iterDoc2.iterator();

                int phe = (int) iterator2.next().get("phe");

                Iterator<Document> iterator3;
                FindIterable<Document> iterDoc3 = collection.find(condicion);
                iterator3 = iterDoc3.iterator();

                String che = (String) iterator3.next().get("che");

                TypedQuery<Horasextra> query =
                        em.createQuery("SELECT h FROM Horasextra h WHERE h.che = '" + che + "'", Horasextra.class);
                List<Horasextra> results = query.getResultList();
                for (Horasextra h : results) {
                    int nhe = h.getNhe();
                    int salarioTotal = sb + phe * nhe + totalFillos * 100;
                    String cadeai="INSERT INTO finalinf(cinf,salariototal) values ("+cinf+","+salarioTotal+")";
                    PreparedStatement ps2 = conexion().prepareStatement(cadeai);
                    ps2.executeUpdate();
                }
            }
        }

        instrucciones.close();
        conexion().close();
    }
}