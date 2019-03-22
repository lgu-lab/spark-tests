package spark.dataset;

public class Person {

	private int id ;
	private String firtName ; 
	private String lastName ;
	
	
	public Person(int id, String firtName, String lastName) {
		super();
		this.id = id;
		this.firtName = firtName;
		this.lastName = lastName;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getFirtName() {
		return firtName;
	}
	public void setFirtName(String firtName) {
		this.firtName = firtName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	@Override
	public String toString() {
		return "Person [id=" + id + ", firtName=" + firtName + ", lastName=" + lastName + "]";
	} 

}
