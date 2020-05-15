import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.opencsv.CSVReader;
import org.bson.Document;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

public class ImportMongoDb {
    public static void insertData(MongoCollection mongo) throws IOException {
        List<InsertOneModel<Document>> documents = new ArrayList<>();
        Document document;
        @SuppressWarnings("deprecation")
        CSVReader reader = new CSVReader(new FileReader("D:\\Download\\1589518259000数据分析12\\数据分析12\\database\\database\\task2\\src\\Buildings.csv"), '\t');
        String[] list1 = reader.readNext();
        int loop = 0;
        int bufferSize = 5000;
        while ((list1 = reader.readNext()) != null) {
            String[] list = list1[0].split(",");
            if (list.length < 19) continue;
            documents.add(new InsertOneModel<>(document = new Document("Census year", list[0]).append("Block ID", list[1])
                    .append("Property ID", list[2]).append("Base property ID", list[3]).append("Building name", list[4])
                    .append("Street address", list[5]).append("CLUE small area", list[6]).append("Construction year", list[7])
                    .append("Refurbished year", list[8]).append("Number of floors", list[9]).append("Predominant space use", list[10])
                    .append("Accessibility type", list[11]).append("Accessibility type description", list[12]).append("Accessibility rating", list[13])
                    .append("Bicycle spaces", list[14]).append("Has showers", list[15]).append("x coordinate", list[16])
                    .append("y coordinate", list[17]).append("Location", list[18])));
            if (++loop % bufferSize == 0) {
                mongo.bulkWrite(documents, new BulkWriteOptions());
                documents.clear();
            }
            System.out.println(loop);
        }
        mongo.bulkWrite(documents, new BulkWriteOptions());
    }

    //   SELECT * FROM property WHERE BLOCDID ='45' and PROPERTYID < '101147'
//   db.Buildings.find({"Block ID" : '45',"Property ID":{$lt:"101147"}})
    static Block<Document> printBlock = new Block<Document>() {
        @Override
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };

    public static void query(MongoCollection mongo, int index) {
        switch (index) {
            case 0:
                mongo.find(and(eq("Block ID", "45"), lt("Property ID", "101147")));
                break;
            case 1:
                mongo.find(and(eq("Number of floors", "8"), lt("Property ID", "101190")));
                break;
            case 2:
                mongo.find(and(eq("Street address", "101 Barkly Street"), eq("Number of floors", "1")));
                break;
            case 3:
                mongo.find(eq("Street address", "101 Barkly Street"));
                break;
            default:
                break;

        }
//        mongo.find(and(eq("Block ID", "45"), lt("Property ID", "101147")));
//        mongo.find(and(eq("Number of floors", "8"), lt("Property ID", "101190")));
//        mongo.find(and(eq("Street address", "101 Barkly Street"), eq("Number of floors", "1")));
//        mongo.find(eq("Street address", "101 Barkly Street"));

    }


    public static void main(String[] args) throws IOException {
        MongoClient mongoClient = new MongoClient("192.168.1.35", 27017);
        MongoDatabase DB = mongoClient.getDatabase("MongoDB");
        MongoCollection mongColl = DB.getCollection("Buildings");
//        insertData(mongColl);

        long start = System.currentTimeMillis();
        query(mongColl, Integer.parseInt(args[0]));
        long elapsedTime = System.currentTimeMillis() - start;
        float total = elapsedTime / (1000F);
        System.out.println("Finished. time spent: " + total + "seconds");
    }

}
