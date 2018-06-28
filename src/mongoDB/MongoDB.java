package mongoDB;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Projections;

public class MongoDB {
	private static MongoClient mongoClient;
	private static MongoDatabase ufoDB;
	private static MongoCollection<Document> ufoCollection;
	private String data = "/home/hbase/Documents/NoSQLP05/UFOData/ufo-sightings/complete1.json";
	
	public static void main(String[] args) {
		MongoDB mongo = new MongoDB();

	}
	public MongoDB() {
		connectToDB();
//		getCityAndState("_id", "city", "country");
		getCityAndCountry();
		System.out.println("MONGODB");
		
	}
	private void connectToDB() {
		mongoClient = new MongoClient("localhost");
		ufoDB = mongoClient.getDatabase("dbUfo");
		System.out.println("DBName: "+ufoDB.getName());
		ufoCollection = ufoDB.getCollection("Ufo");
	}
	
	private void loadDataInDB(MongoDatabase dbTest) {
		List<InsertOneModel<Document>> docs = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(data))){String line;
		      int count = 0;
		      while ((line = br.readLine()) != null) {
		    	 count++;
		         try{
		        	 docs.add(new InsertOneModel<>(Document.parse(line)));
		         }catch(Exception e) {
			    	 System.out.println(e.getStackTrace()+ " "+line+" " + count); 
		         }
		         ufoCollection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
		         docs.clear();
		    }
		      System.out.println("Einlesen fertig");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public List<String> getPlz(String city, String plz, String state) {
		List<String> listPlzStates = new ArrayList<>();
		List<String> listPlz = new ArrayList<>();	
		List<String> listStates = new ArrayList<>();
		
		BasicDBObject query = new BasicDBObject("city",city);
//		query.put("_id", plz);
		
		BasicDBObject values = new BasicDBObject();
		values.put(plz, "1");
		values.put(state, "1");

		String plzs = "";
		String states = "";
		
		MongoCursor<Document> cursor = ufoCollection.find(query).projection(values).iterator();
		while(cursor.hasNext()) {
			Document doc = cursor.next();
			
//			listPlz.add(doc.getString(plz));
//			listStates.add(doc.getString(state));
	
			plzs = plzs +" "+ doc.getString(plz);
			states += " "+ doc.getString(state);
		}	
		listPlzStates.add(plzs);
		listPlzStates.add(states);

		return listPlzStates;
	}
	/**
	 * 
	 * @return
	 */
	public List<String[]> getCityAndCountry(){
		FindIterable it = ufoCollection.find().projection(Projections.include("city","country"));
		ArrayList<Document> docs = new ArrayList();
		it.into(docs);
	
		String city = "";
		String country = "";
		
		List<String[]> citiesAndCountries = new ArrayList<>();
		
		for(Document doc : docs) {
			city = doc.get("city").toString();
			country = doc.getString("country").toString();
			
			String[] city2 =  city.split("\\(");
			city = city2[0];
				
			if(!country.isEmpty()) {
				//ADD to List
				String[] cityCountryPair = new String [2];
				cityCountryPair[0] = city;
				cityCountryPair[1] = country;
				
				citiesAndCountries.add(cityCountryPair);
//				for(String[] s: citiesAndCountries) {
//					System.out.println(s[0]+" - "+s[1]);
//				}
			}
			
		}
		return citiesAndCountries;
	}
	
}
