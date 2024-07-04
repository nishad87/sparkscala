object assignment1_even_positive {
  def check_even_positive(number:Int):Boolean={
    if( number%2==0 && number>0)
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
  }

}
