import scala.util.control.Breaks

object NthPalindromeLogic04JUL2024 {

  def main (args:Array[String]):Unit={
    println(checkPalindrome(123))
    println(checkPalindrome(222))
    nthPalindromeWithinRange(10,50,5);
    nthPalindromeWithinRange(10,100,5);

  }

  def nthPalindromeWithinRange(pLowerBound:Int , pUpperBound:Int , n:Int):Unit={
    var counter:Int = 0;
    var nPalindrome:Int = 0;
    var loop = new Breaks
    loop.breakable {
      for (i <- pLowerBound to pUpperBound) {
        if (checkPalindrome(i) == true) {
          counter = counter + 1
          if (counter == n) {
            nPalindrome = i
            loop.break;
          }
        }
      }
    }
    if (nPalindrome!=0)
      {
        println("NthPalindrome found within range " + pLowerBound + ":" + pUpperBound + " =  "+  nPalindrome)
      }
    else
      {
        println("No Palindrome found within range " + pLowerBound + ":" + pUpperBound)

      }
  }

  def checkPalindrome (pNum:Int):Boolean={
    /*
    1234
    123/10

    3
    32
    */
    var factor = 10;
    var num:Int = pNum;
    var rem = 0;
    var sum = 0;
    while (num!=0)
      {
        rem = num%factor
        sum = (sum * factor ) + rem;
        num = num/factor;
      }

    if (pNum == sum )
      {
        return true
      }
    return false;

  }

}
