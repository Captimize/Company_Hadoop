package com.fruit;

//public class MyFruit {
public class MyFruit  implements Comparable<MyFruit>{

	private String fruitName;
	private String fruitDesc;
	private int quantity;

	public MyFruit(String fruitName, String fruitDesc, int quantity) {
		super();
		this.fruitName = fruitName;
		this.fruitDesc = fruitDesc;
		this.quantity = quantity;
	}

	public String getFruitName() {
		return fruitName;
	}
	public void setFruitName(String fruitName) {
		this.fruitName = fruitName;
	}
	public String getFruitDesc() {
		return fruitDesc;
	}
	public void setFruitDesc(String fruitDesc) {
		this.fruitDesc = fruitDesc;
	}
	public int getQuantity() {
		return quantity;
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
		public int compareTo(MyFruit compareFruit) {
	
			int compareQuantity = ((MyFruit) compareFruit).getQuantity();
	
			//ascending order
			return this.quantity - compareQuantity;
	
			//descending order
			//return compareQuantity - this.quantity;
		}

	
}