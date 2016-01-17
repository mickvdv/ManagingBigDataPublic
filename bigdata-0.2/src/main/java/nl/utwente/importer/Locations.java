package nl.utwente.importer;

public class Locations {
	
	public String id;
	public String latitude;
	public String longitude;
	
	public Locations(String id, String latitude, String longitude){
		this.id = id;
		this.latitude = latitude;
		this.longitude = longitude;
	}
	
	public String getId(){
		return id;
	}
	public String getLatitude(){
		return latitude;
	}
	
	public String getLongitude(){
		return longitude;
	}
}
