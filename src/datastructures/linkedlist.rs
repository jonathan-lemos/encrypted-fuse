use std::cell::RefCell;
use std::rc::{Rc, Weak};

#[derive(Debug)]
struct LinkedListNode<T> {
    previous: Option<Weak<RefCell<LinkedListNode<T>>>>,
    current: Rc<T>,
    next: Option<Rc<RefCell<LinkedListNode<T>>>>,
}

pub struct LinkedList<T> {
    head: Option<Rc<RefCell<LinkedListNode<T>>>>,
    tail: Option<Rc<RefCell<LinkedListNode<T>>>>,
}

#[derive(Debug)]
pub struct LinkedListNodeRef<T>(Rc<RefCell<LinkedListNode<T>>>);

impl<T: Clone> LinkedListNodeRef<T> {
    pub fn value(&self) -> Rc<T> {
        self.0.borrow().current.clone()
    }
}

impl<T> LinkedList<T> {
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }

    pub fn append(&mut self, value: T) -> LinkedListNodeRef<T> {
        match &self.tail {
            Some(node) => {
                let new_node = Rc::new(RefCell::new(LinkedListNode {
                    previous: Some(Rc::downgrade(&node)),
                    current: Rc::new(value),
                    next: None,
                }));
                node.borrow_mut().next = Some(new_node.clone());
                self.tail = Some(new_node.clone());
                LinkedListNodeRef(new_node)
            }
            None => {
                let new_node = Rc::new(RefCell::new(LinkedListNode {
                    previous: None,
                    current: Rc::new(value),
                    next: None,
                }));
                self.head = Some(new_node.clone());
                self.tail = Some(new_node.clone());
                LinkedListNodeRef(new_node)
            }
        }
    }

    pub fn head(&self) -> Option<LinkedListNodeRef<T>> {
        self.head
            .as_ref()
            .map(|node| LinkedListNodeRef(node.clone()))
    }

    pub fn remove(&mut self, node: LinkedListNodeRef<T>) {
        let prev = node
            .0
            .borrow()
            .previous
            .clone()
            .and_then(|node| node.upgrade());

        let next = node.0.borrow().next.clone();

        if let Some(prev) = prev.clone() {
            prev.borrow_mut().next = next.clone();
        }

        if let Some(next) = next.clone() {
            next.borrow_mut().previous = prev.map(|node| Rc::downgrade(&node));
        }

        if let Some(head) = self.head.clone()
            && Rc::ptr_eq(&head, &node.0)
        {
            self.head = head.borrow().next.clone()
        }

        if let Some(tail) = self.tail.clone()
            && Rc::ptr_eq(&tail, &node.0)
        {
            self.tail = tail
                .borrow()
                .previous
                .clone()
                .and_then(|node| node.upgrade());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::{assert_none, assert_some};
    use std::fmt::Debug;

    fn create<T, I: IntoIterator<Item = T>>(
        elems: I,
    ) -> (LinkedList<T>, Vec<LinkedListNodeRef<T>>) {
        let mut linked_list = LinkedList::new();
        let mut nodes = Vec::new();
        for elem in elems.into_iter() {
            nodes.push(linked_list.append(elem))
        }
        (linked_list, nodes)
    }

    fn iter<T: Clone + Debug>(linked_list: LinkedList<T>) -> Vec<T> {
        let mut prev = None;
        let mut ptr = linked_list.head.clone();
        let mut vec = Vec::new();

        while let Some(current) = ptr.clone() {
            if let Some(prev_node) = prev.clone() {
                let current_prev_weak = assert_some!(current.borrow().previous.clone());
                let current_prev = assert_some!(current_prev_weak.upgrade());
                assert!(Rc::ptr_eq(&current_prev, &prev_node));
            } else {
                assert_none!(current.borrow().previous);
            }

            vec.push(current.borrow().current.as_ref().clone());
            prev = ptr.clone();
            ptr = current.borrow().next.clone();
        }

        if let Some(prev_node) = prev.clone() {
            assert!(Rc::ptr_eq(&prev_node, &assert_some!(linked_list.tail)));
        } else {
            assert_none!(linked_list.tail);
        }

        vec
    }

    #[test]
    fn test_head() {
        let (linked_list, nodes) = create([1, 2, 3]);
        let head = assert_some!(linked_list.head());
        assert_eq!(head.value(), Rc::new(1));
    }

    #[test]
    fn test_iter() {
        let (linked_list, nodes) = create([1, 2, 3]);
        let values = iter(linked_list);
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn test_iter_empty() {
        let linked_list = LinkedList::<i32>::new();
        let values = iter(linked_list);
        assert_eq!(values, &[]);
    }

    #[test]
    fn remove_from_beginning() {
        let (mut linked_list, mut nodes) = create([1, 2, 3]);
        linked_list.remove(nodes.remove(0));

        let values = iter(linked_list);
        assert_eq!(values, &[2, 3]);
    }

    #[test]
    fn remove_from_end() {
        let (mut linked_list, mut nodes) = create([1, 2, 3]);
        linked_list.remove(nodes.remove(2));

        let values = iter(linked_list);
        assert_eq!(values, &[1, 2]);
    }

    #[test]
    fn remove_from_middle() {
        let (mut linked_list, mut nodes) = create([1, 2, 3]);
        linked_list.remove(nodes.remove(1));

        let values = iter(linked_list);
        assert_eq!(values, &[1, 3]);
    }

    #[test]
    fn remove_only_element() {
        let (mut linked_list, mut nodes) = create([1]);
        linked_list.remove(nodes.remove(0));

        let values = iter(linked_list);
        assert_eq!(values, &[]);
    }
}
