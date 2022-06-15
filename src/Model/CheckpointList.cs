using System;
using System.Threading.Tasks;

namespace LightMessager.Model
{
    /// <summary>
    /// 因为rabbitmq返回消息confirm信息的特殊性，这里采用了类似链表的数据结构
    /// </summary>
    public class CheckpointList
    {
        private class Node
        {
            // Each node has a reference to the next node in the list. 
            public Node Next;
            // Each node holds a value of type T. 
            public ulong DeliveryTag;
            public TaskCompletionSource<bool> Tcs;
        }

        // The list is initially empty. 
        private Node head = null;

        // Add a node at the beginning of the list with t as its data value. 
        public void AddNode(ulong deliveryTag, TaskCompletionSource<bool> tcs)
        {
            Node newNode = new Node();
            newNode.Next = head;
            newNode.DeliveryTag = deliveryTag;
            newNode.Tcs = tcs;
            head = newNode;
        }

        /// <summary>
        /// for debug only
        /// </summary>
        internal void Travel()
        {
            Node current = head;
            while (current != null)
            {
                Console.Write(current.DeliveryTag + ", ");
                current = current.Next;
            }
        }

        public void CheckUpTo(ulong deliveryTag)
        {
            Node pre = null;
            Node current = head;
            while (current != null)
            {
                if (deliveryTag >= current.DeliveryTag)
                {
                    if (pre == null)
                    {
                        head = null;
                        break;
                    }
                    pre.Next = null;
                    break;
                }
                pre = current;
                current = current.Next;
            }
            while (current != null)
            {
                current.Tcs.SetResult(true);
                current = current.Next;
            }
        }

        public void Check(ulong deliveryTag)
        {
            Node pre = null;
            Node current = head;
            while (current != null)
            {
                if (deliveryTag == current.DeliveryTag)
                {
                    current.Tcs.SetResult(true);
                    if (pre == null)
                    {
                        head = null;
                        break;
                    }
                    pre.Next = current.Next;
                    break;
                }
                pre = current;
                current = current.Next;
            }
        }

        public void Check()
        {
            Node current = head;
            while (current != null)
            {
                current.Tcs.SetResult(true);
                current = current.Next;
            }
        }
    }
}
