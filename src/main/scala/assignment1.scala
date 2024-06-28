import scala.math.abs
import scala.util.control._
object assignment1 {
  def check_even_positive(number:Int):Boolean={
    if( number%2==0 && number>0)
    {
      return true;
    }
    return false;
  }

  def range_check_or(number:Int):Boolean={
    if( number< -10 || number > 10)
    {
      return true;
    }
    return false;
  }


  def odd_number_check_with_and(number:Int):Boolean={
    if( number%2==1 && number%3==1)
    {
      return true;
    }
    return false;
  }

  def divisibleBy4Or6(number:Int):Boolean={
    if( number%4==0 || number%6==0)
    {
      return true;
    }
    return false;
  }

  def eligibleToVoteOrDrive(age:Int):Boolean={
    if( age>=18 || age>=16)
    {
      return true;
    }
    return false;
  }

  def multipleRangeCheck(number:Int):Boolean={
    if( (number>=1 && number<=10) || (number>=20 && number<=30))
    {
      return true;
    }
    return false;
  }

  def negativeAndOdd(number:Int):Boolean={
    if(number<0 && (number%2).abs==1)
    {
      return true;
    }
    return false;
  }

  def checkSeniorOrStudent(age:Int):Boolean={
    if(age>60 || age<25)
    {
      return true;
    }
    return false;
  }

  def checkDivisibleBy5And7(number:Int):Boolean={
    if(number%5==0 && number%7==0)
    {
      return true;
    }
    return false;
  }

  def checkNonNegativeOrEven(number:Int):Boolean={
    if(number%2==0 || number>0)
    {
      return true;
    }
    return false;
  }

  def checkOdd(number:Int):Boolean={
    if(number%2==1)
      {
        return true;
      }
      return false;
  }
  def checkPrime(number:Int):Boolean={
    val checkVal = (number/2).toInt;
    var i = 2;
    var isPrime = true;
    val loop = new Breaks;
    loop.breakable {
      while (i <= checkVal) {
        if (number % i == 0) {
          isPrime = false;
          loop.break();
        }
        i=i+1;

      }
    }
    return isPrime;
  }

  def checkPrimeAndOddNumber(number:Int):Boolean={
    if(checkPrime(number) && checkOdd(number))
    {
      return true;
    }
    return false;
  }

  def main (args:Array[String]):Unit={
    println(check_even_positive(2));
    println(check_even_positive(-2));
    println(check_even_positive(0));
    println(check_even_positive(3));
    println(check_even_positive(-3));
    println("########Output2#######");
    println(range_check_or(2));
    println(range_check_or(-2));
    println(range_check_or(0));
    println(range_check_or(3));
    println(range_check_or(-3));
    println(range_check_or(10));
    println(range_check_or(11));
    println(range_check_or(-10));
    println(range_check_or(-11));
    println("########Output3#######");
    println(odd_number_check_with_and(27));
    println(odd_number_check_with_and(31));
    println("########Output4#######");
    println(divisibleBy4Or6(44));
    println(divisibleBy4Or6(42));
    println(divisibleBy4Or6(46));
    println("########Output5#######");
    println(eligibleToVoteOrDrive(15));
    println(eligibleToVoteOrDrive(20));
    println("########Output6#######");
    println(multipleRangeCheck(11));
    println(multipleRangeCheck(2));
    println(multipleRangeCheck(20));
    println("########Output7#######");
    println(negativeAndOdd(-7));
    println("########Output9#######");
    println(checkDivisibleBy5And7(35));
    println(checkDivisibleBy5And7(30));
    println("########Output11#######");
    println(checkPrimeAndOddNumber(17));
    println(checkPrimeAndOddNumber(61));
    println(checkPrimeAndOddNumber(117));
    println(checkPrimeAndOddNumber(200));
  }

}
