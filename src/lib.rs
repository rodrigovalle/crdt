use std::collections::HashMap;
use std::convert::TryInto;
use std::cmp::max;

/// An eventually consistent distributed counter that only grows.
#[derive(Debug)]
pub struct GCounter {
    /// Map from ReplicaID to the replica's local count.
    counters: HashMap<String, u64>,
}

impl GCounter {
    pub fn new() -> GCounter {
        GCounter {
            counters: HashMap::new(),
        }
    }

    pub fn value(&self) -> u64 {
        self.counters.values().sum()
    }

    pub fn merge(&mut self, other: GCounter) {
        let mut new_counts = vec![];
        for (k, v_other) in other.counters.into_iter() {
            if let Some(v_local) = self.counters.get_mut(&k) {
                *v_local = max(*v_local, v_other);
            } else {
                new_counts.push((k, v_other));
            }
        }

        for (k, new_count) in new_counts.into_iter() {
            self.counters.insert(k, new_count);
        }
    }

    pub fn inc(&mut self, replica: String, count: u64) {
        self.counters.entry(replica)
            .and_modify(|v| { *v += count })
            .or_insert(count);
    }
}

#[derive(Debug)]
pub struct PNCounter {
    inc: GCounter,
    dec: GCounter,
}

impl PNCounter {
    pub fn new() -> PNCounter {
        PNCounter {
            inc: GCounter::new(),
            dec: GCounter::new(),
        }
    }

    pub fn value(&self) -> i64 {
        (self.inc.value() - self.dec.value()).try_into().expect("overflow")
    }

    pub fn merge(&mut self, other: PNCounter) {
        self.inc.merge(other.inc);
        self.dec.merge(other.dec);
    }

    pub fn inc(&mut self, replica: String, count: u64) {
        self.inc.inc(replica, count);
    }

    pub fn dec(&mut self, replica: String, count: u64) {
        self.dec.inc(replica, count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashmap;

    #[test]
    fn test_gcounter() {
        let mut counter_a = GCounter::new();
        counter_a.inc("a".to_string(), 13);
        counter_a.inc("b".to_string(), 20);

        let mut counter_b = GCounter::new();
        counter_b.inc("a".to_string(), 10);
        counter_b.inc("b".to_string(), 21);

        counter_a.merge(counter_b);
        assert_eq!(counter_a.counters, hashmap!{
            "a".to_string() => 13,
            "b".to_string() => 21,
        });
        assert_eq!(counter_a.value(), 34);
    }

    #[test]
    fn test_pncounter() {
        let mut counter_a = PNCounter::new();
        counter_a.inc("a".to_string(), 10);
        counter_a.dec("a".to_string(), 2);
        counter_a.inc("b".to_string(), 12);

        let mut counter_b = PNCounter::new();
        counter_b.inc("a".to_string(), 10);
        counter_b.inc("b".to_string(), 12);
        counter_b.dec("b".to_string(), 2);

        counter_a.merge(counter_b);
        println!("{:#?}", counter_a);
        assert_eq!(counter_a.value(), 18);
    }
}
