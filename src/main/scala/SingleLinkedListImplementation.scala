object SingleLinkedListImplementation {
  class Node[T] (
    var data: T,
    var next: Option[Node[T]] = None
  )

  class SingleLinkedList[T]
  {

  var head : Option[Node[T]] = None;

  def append(data:T):Unit={
    val newnode = new Node(data);

    if (head.isEmpty)
      {
        head = Some(newnode)
      }
      else
      {
        //head.next = Some(newnode)
        var current = head
        while (current.get.next.isDefined)
          {
            current = current.get.next
          }
          current.get.next = Some(newnode)
      }
  }

    def delete(data:T):Unit= {

    }

    def deleteNode(head:Option[Node[T]],data:T):Unit={
      var current = head;
      var previous : Option[Node[T]] = None
      //while (current.get.next.isDefined && current.get.data!=data)
      while (current.isDefined && current.get.data!=data)
        {
          previous = current
          current = current.get.next;

        }

      if (previous.isEmpty)
        {
          //head =

        }

        previous.get.next = current.get.next
    }

  def displayLinkedList():Unit={
    var current = head
    while (current.get.next.isDefined)
      {
        println("Node value -> "+ current.get.data)
        current = current.get.next
      }
    println("Node value -> "+ current.get.data)
  }


  }


  def main(args: Array[String]): Unit = {
  var implobj = new SingleLinkedList[Int]
  implobj.append((10))
    implobj.append((20))
    implobj.append((30))
    implobj.append((40))
    implobj.displayLinkedList()
  }
}
