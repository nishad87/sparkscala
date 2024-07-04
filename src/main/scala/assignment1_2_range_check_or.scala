object assignment1_2_range_check_or {
  def range_check_or(number:Int):Boolean={
    if( number< -10 || number > 10)
    {
      return true;
    }
    return false;
  }
  def main (args:Array[String]):Unit={
    println(range_check_or(2));
    println(range_check_or(-2));
    println(range_check_or(0));
    println(range_check_or(3));
    println(range_check_or(-3));
    println(range_check_or(10));
    println(range_check_or(11));
    println(range_check_or(-10));
    println(range_check_or(-11));
  }

}
