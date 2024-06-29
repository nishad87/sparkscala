object PatternRelatedPrograms {
  def rightAnglePattern():Unit={
    for(i<-1 to 5)
      {
        for (j<-1 to i)
          {
            print('*')
          }
      println()
      }
  }

  def squarePattern():Unit={
    for(i<-1 to 4)
    {
      for (j<-1 to 4)
      {
        print('*')
      }
      println()
    }
  }

  def invertedTrianglePattern():Unit={
    for(i<-5 to 1 by -1 )
    {
      for (j<-1 to i)
      {
        print('*')
      }
      println()
    }
  }

  def rightAnglePatternWithUnderscore():Unit={
    for(i<-1 to 5)
    {
      for (j<-1 to i)
      {
        if (i!=j) {
          print("*_")
        }
        else
          {
            print('*')
          }
      }
      println()
    }
  }

  def pattern9():Unit={
    for(i<-1 to 5)
    {
      for (j<-1 to i)
      {
        print('*')
      }
      println()
    }
    for(i<-4 to 1 by -1)
    {
      for (j<-1 to i)
      {
        print('*')
      }
      println()
    }
  }

  def butterFlyPattern():Unit={
    val num:Int = 3;

    for (i<-1 to num)
      {
        for (j<-1 to i)
          {
            print("* ")
          }
        for(j<-i to num-1)
          {
          print("  ")
          }
          for(j<-i to num-1)
          {
          print("  ")
          }
        for (j<-1 to i)
        {
          print("* ")
        }
      println()
      }
      for (i<-1 to (num-1))
        {
          for (j<-i to (num-1))
            {
              print("* ")
            }
          for (j<-1 to i)
          {
            print("  ")
          }
          for (j<-1 to i)
          {
            print("  ")
          }
          for (j<-i to (num-1))
          {
            print("* ")
          }
          println()
        }



  }

  def main (args:Array[String]):Unit={
    rightAnglePattern()
    println("-----------------------------------------------------")
    squarePattern()
    println("-----------------------------------------------------")
    invertedTrianglePattern()
    println("-----------------------------------------------------")
    rightAnglePatternWithUnderscore
    println("-----------------------------------------------------Pattern 9 ")
    pattern9()
    println("-----------------------------------------------------ButterFly Pattern ")
    butterFlyPattern
  }
}
