package com.fruit;

import java.util.Arrays;

public class MyFruitSortRun{

	public static void main(String args[]){

		MyFruit[] fruits = new MyFruit[4];

		MyFruit pineappale = new MyFruit("Pineapple", "Pineapple description",70);
		MyFruit apple = new MyFruit("Apple", "Apple description",100);
		MyFruit orange = new MyFruit("Orange", "Orange description",80);
		MyFruit banana = new MyFruit("Banana", "Banana description",90);
		
		fruits[0]=pineappale;
		fruits[1]=apple;
		fruits[2]=orange;
		fruits[3]=banana;

		Arrays.sort(fruits);

		int i=0;
		for(MyFruit temp: fruits){
		   System.out.println("fruits " + ++i + " : " + temp.getFruitName() +
			", Quantity : " + temp.getQuantity());
		}

	}
}
