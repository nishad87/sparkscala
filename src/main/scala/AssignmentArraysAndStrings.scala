object AssignmentArraysAndStrings {

def sumArrayElements(arr:Array[Int]):Int=
  {
    var sum = 0
    for (i<-0 to arr.length-1)
      {
        sum = sum + arr(i)
      }
    sum
  }

  def reverseArrayElements(arr:Array[String]):Array[String]={
    var start:Int = 0;
    var end:Int = arr.length-1;
    var temp:String = "";
    while (start < end ){
      temp = arr(start);
      arr(start) = arr(end);
      arr(end) = temp;
      start = start + 1;
      end = end - 1;
    }
    arr

  }

  def avgArrayElements(arr:Array[Int]):Float={
    var avg:Float = 0;
    var sum = 0
    for (i<-0 to arr.length - 1)
      {
        sum = sum + arr(i);
      }
      avg = sum / arr.length;
      avg

  }


  def mergeArray(arr1:Array[Int] , arr2:Array[Int]):Array[Int]={
    val lengthArr1:Int = arr1.length
    val lengthArr2:Int = arr2.length

    var targetArray = new Array[Int](lengthArr1+lengthArr2)

    for (i<-0 to arr1.length - 1)
    {
      targetArray(i) = arr1(i)
    }


    for (j<-0 to arr2.length - 1)
    {
      targetArray(j+lengthArr1) = arr2(j)
    }
    targetArray
  }
  def main (args:Array[String]):Unit={
//Q1
var intArray = Array(1,2,3,4,5);
    var intArray2 = Array(6,7,8);
    var strArray = Array("Nishad","Mohan","Narse")
    println(sumArrayElements(intArray))
    reverseArrayElements(strArray).map(println)
    println("Avg"+avgArrayElements(intArray));
    println("Merged array"+mergeArray(intArray,intArray2).map(println));
}
}
