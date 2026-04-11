use crate::data_structures::linked_list::{LinkedList, LinkedListNodeRef};
use std::collections::HashMap;
use std::hash::Hash;

pub struct CacheMap<K, V, E>
where
    K: Hash + Eq + Clone,
{
    create_fn: Box<dyn FnMut(&K) -> Result<V, E>>,
    drop_fn: Box<dyn FnMut(&K, &mut V) -> Result<(), E>>,
    linked_list: LinkedList<K>,
    entries: HashMap<K, V>,
    linked_list_nodes: HashMap<K, LinkedListNodeRef<K>>,
    max_capacity: usize,
}

impl<K, V, E> CacheMap<K, V, E>
where
    K: Hash + Eq + Clone,
{
    pub fn new<C, D>(capacity: usize, create_fn: C, drop_fn: D) -> Self
    where
        C: FnMut(&K) -> Result<V, E> + 'static,
        D: FnMut(&K, &mut V) -> Result<(), E> + 'static,
    {
        Self {
            create_fn: Box::new(create_fn),
            drop_fn: Box::new(drop_fn),
            linked_list: LinkedList::new(),
            linked_list_nodes: HashMap::with_capacity(capacity),
            entries: HashMap::with_capacity(capacity),
            max_capacity: capacity,
        }
    }

    fn remove_from_linked_list(&mut self, key: &K) {
        if let Some(entry) = self.linked_list_nodes.remove(key) {
            self.linked_list.remove(entry);
        }
    }

    fn append_to_linked_list(&mut self, key: &K) {
        let entry = self.linked_list.append(key.clone());
        self.linked_list_nodes.insert(key.clone(), entry);
    }

    fn send_to_back_of_linked_list(&mut self, key: &K) {
        self.remove_from_linked_list(key);
        self.append_to_linked_list(key);
    }

    fn populate_if_necessary(&mut self, key: &K) -> Result<(), E> {
        if self.entries.contains_key(key) {
            self.send_to_back_of_linked_list(key);
            return Ok(());
        }

        if self.entries.len() >= self.max_capacity {
            let to_evict = self.linked_list.head().unwrap();
            let to_evict_rc = to_evict.value();
            let key_to_evict = to_evict_rc.as_ref();

            (self.drop_fn)(key_to_evict, self.entries.get_mut(key_to_evict).unwrap())?;

            self.remove_from_linked_list(key_to_evict);
            self.entries.remove(key_to_evict);
        }

        let new_entry = (self.create_fn)(key)?;
        self.entries.insert(key.clone(), new_entry);
        self.send_to_back_of_linked_list(key);

        Ok(())
    }

    pub fn try_get(&mut self, key: &K) -> Result<&V, E> {
        self.populate_if_necessary(key)?;
        Ok(self.entries.get(key).unwrap())
    }

    pub fn try_get_mut(&mut self, key: &K) -> Result<&mut V, E> {
        self.populate_if_necessary(key)?;
        Ok(self.entries.get_mut(key).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::{assert_err, assert_ok};
    use quickcheck_macros::quickcheck;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[quickcheck]
    fn test_correctly_computes_entries(nums: Vec<i32>) {
        let mut map = CacheMap::new(4, |k: &i32| Ok::<i32, ()>(k.wrapping_add(1)), |_, _| Ok(()));

        for num in nums {
            let result = assert_ok!(map.try_get(&num));
            assert_eq!(*result, num.wrapping_add(1));
        }
    }

    #[test]
    fn test_keeps_mutations_while_in_cache() {
        let mut map = CacheMap::new(4, |k| Ok::<i32, ()>(k + 1), |_, _| Ok(()));

        *map.try_get_mut(&42).unwrap() = 69;
        assert_eq!(*map.try_get(&42).unwrap(), 69);
    }

    #[test]
    fn test_drops_mutations_when_evicted() {
        let mut map = CacheMap::new(4, |k| Ok::<i32, ()>(k + 1), |_, _| Ok(()));

        *assert_ok!(map.try_get_mut(&42)) = 69;
        assert_ok!(map.try_get(&1));
        assert_ok!(map.try_get(&2));
        assert_ok!(map.try_get(&3));
        // Adding a 5th entry will drop the least recently used entry, which is 42
        assert_ok!(map.try_get(&4));

        assert_eq!(*assert_ok!(map.try_get(&42)), 43);
    }

    #[test]
    fn test_does_not_recreate_if_in_cache() {
        let create_count = Arc::new(AtomicUsize::new(0));

        let create_count_clone = create_count.clone();
        let mut map = CacheMap::new(
            4,
            move |k| {
                create_count_clone.fetch_add(1, Ordering::Relaxed);
                Ok::<i32, ()>(k + 1)
            },
            |_, _| Ok(()),
        );

        assert_ok!(map.try_get_mut(&1));
        assert_ok!(map.try_get_mut(&2));
        assert_ok!(map.try_get_mut(&3));
        assert_ok!(map.try_get_mut(&4));
        assert_ok!(map.try_get_mut(&1));
        assert_ok!(map.try_get_mut(&2));
        assert_ok!(map.try_get_mut(&3));
        assert_ok!(map.try_get_mut(&4));

        assert_eq!(create_count.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn test_drops_least_recently_used() {
        let dropped = Arc::new(Mutex::new(Vec::new()));

        let dropped_clone = dropped.clone();
        let mut map = CacheMap::new(
            4,
            |k: &i32| Ok::<i32, ()>(k + 1),
            move |k, _| {
                dropped_clone.lock().unwrap().push(k.clone());
                Ok(())
            },
        );

        assert_ok!(map.try_get_mut(&1));
        assert_ok!(map.try_get_mut(&2));
        assert_ok!(map.try_get_mut(&3));
        assert_ok!(map.try_get_mut(&4));

        assert_ok!(map.try_get_mut(&1));
        assert_ok!(map.try_get_mut(&3));

        assert_ok!(map.try_get_mut(&5));

        assert_eq!(dropped.lock().unwrap().as_ref(), vec![2]);
    }

    #[test]
    fn test_fails_if_create_fn_fails() {
        let mut map = CacheMap::new(
            4,
            |k: &i32| {
                if *k == 2 { Err(()) } else { Ok(k + 1) }
            },
            |_, _| Ok(()),
        );

        assert_ok!(map.try_get_mut(&1));
        assert_err!(map.try_get_mut(&2));
        assert_ok!(map.try_get_mut(&3));
    }

    #[test]
    fn test_fails_if_drop_fn_fails() {
        let mut map = CacheMap::new(
            4,
            |k: &i32| Ok(k + 1),
            |k, _| {
                if *k == 2 { Err(()) } else { Ok(()) }
            },
        );

        assert_ok!(map.try_get_mut(&1));
        assert_ok!(map.try_get_mut(&2));
        assert_ok!(map.try_get_mut(&3));
        assert_ok!(map.try_get_mut(&4));
        assert_ok!(map.try_get_mut(&5));
        assert_err!(map.try_get_mut(&6));
    }
}
