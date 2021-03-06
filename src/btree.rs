// use bitflags::bitflags;
use crate::ref_stack::RefStack;
use crate::slab::{SlabAllocator, SlabBox};
use core::any::type_name;
use core::borrow::Borrow;
use core::cmp::Ordering;
use core::fmt;
use core::mem::{self, size_of, ManuallyDrop, MaybeUninit};
use core::ptr;
use core::slice;

const B: usize = 10;

const MIN_NUM_ELEMENTS: usize = B - 1;
const MAX_NUM_ELEMENTS: usize = 2 * B - 1;
// const MIN_NUM_CHILDREN: usize = B;
const MAX_NUM_CHILDREN: usize = 2 * B;

trait Child<K, V> {
    fn len(&self) -> usize;

    fn keys(&self) -> &[K];
    fn values(&self) -> &[V];

    fn keys_mut(&mut self) -> &mut [K];
    fn values_mut(&mut self) -> &mut [V];

    unsafe fn get_key_unchecked(&self, i: usize) -> &K;
    unsafe fn get_value_unchecked(&self, i: usize) -> &V;

    unsafe fn get_key_mut_unchecked(&mut self, i: usize) -> &mut K;
    unsafe fn get_value_mut_unchecked(&mut self, i: usize) -> &mut V;

    // #[inline]
    fn linsearch<Q>(&self, key: &Q) -> Result<usize, usize>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        for (i, x) in self.keys().iter().enumerate() {
            let ordering = key.cmp(x.borrow());
            if ordering == Ordering::Less {
                return Err(i);
            } else if ordering == Ordering::Equal {
                return Ok(i);
            }
            // if ordering == Ordering::Greater {
            //     continue;
            // } else if ordering == Ordering::Less {
            //     return Err(i);
            // } else {
            //     // ordering == Ordering::Equal
            //     return Ok(i);
            // }
        }
        Err(self.len())
    }
}

#[repr(align(8))]
struct Leaf<K, V> {
    len: u8,
    keys: [MaybeUninit<K>; MAX_NUM_ELEMENTS],
    values: [MaybeUninit<V>; MAX_NUM_ELEMENTS],
}

impl<K, V> Child<K, V> for Leaf<K, V> {
    #[inline]
    fn len(&self) -> usize {
        self.len as _
    }

    #[inline]
    fn keys(&self) -> &[K] {
        unsafe { slice::from_raw_parts(self.keys.as_ptr() as _, self.len()) }
    }
    #[inline]
    fn values(&self) -> &[V] {
        unsafe { slice::from_raw_parts(self.values.as_ptr() as _, self.len()) }
    }

    #[inline]
    fn keys_mut(&mut self) -> &mut [K] {
        unsafe { slice::from_raw_parts_mut(self.keys.as_mut_ptr() as _, self.len()) }
    }
    #[inline]
    fn values_mut(&mut self) -> &mut [V] {
        unsafe { slice::from_raw_parts_mut(self.values.as_mut_ptr() as _, self.len()) }
    }

    #[inline]
    unsafe fn get_key_unchecked(&self, i: usize) -> &K {
        self.keys.get_unchecked(i).assume_init_ref()
    }
    #[inline]
    unsafe fn get_value_unchecked(&self, i: usize) -> &V {
        self.values.get_unchecked(i).assume_init_ref()
    }

    #[inline]
    unsafe fn get_key_mut_unchecked(&mut self, i: usize) -> &mut K {
        self.keys.get_unchecked_mut(i).assume_init_mut()
    }
    #[inline]
    unsafe fn get_value_mut_unchecked(&mut self, i: usize) -> &mut V {
        self.values.get_unchecked_mut(i).assume_init_mut()
    }
}

impl<K, V> Leaf<K, V> {
    #[inline]
    fn new(alloc: &mut SlabAllocator<Self>) -> SlabBox<Self> {
        unsafe {
            let mut slf = SlabBox::uninit(alloc).assume_init();
            slf.len = 0;
            slf
        }
    }

    #[inline]
    fn get_all_mut(&mut self) -> (&mut [K], &mut [V]) {
        let len = self.len();
        unsafe {
            (
                &mut *(&mut self.keys[..len] as *mut _ as *mut _),
                &mut *(&mut self.values[..len] as *mut _ as *mut _),
            )
        }
    }

    #[inline]
    fn push(&mut self, key: K, value: V) {
        debug_assert_ne!(self.len(), MAX_NUM_ELEMENTS);

        self.keys[self.len()].write(key);
        self.values[self.len()].write(value);
        self.len += 1;
    }

    fn unshift(&mut self, key: K, value: V) {
        debug_assert_ne!(self.len(), MAX_NUM_ELEMENTS);

        unsafe {
            ptr::copy(
                self.keys.as_ptr(),
                self.keys.as_mut_ptr().add(1),
                self.len(),
            );
            ptr::copy(
                self.values.as_ptr(),
                self.values.as_mut_ptr().add(1),
                self.len(),
            );
            self.len += 1;
            self.keys[0].write(key);
            self.values[0].write(value);
        }
    }

    fn insert(&mut self, idx: usize, key: K, value: V) -> Option<(K, V)> {
        debug_assert!(idx <= self.len());
        if idx == MAX_NUM_ELEMENTS {
            return Some((key, value));
        }
        let overflow;
        if self.len() == MAX_NUM_ELEMENTS {
            self.len -= 1;
            overflow = unsafe {
                Some((
                    self.keys[self.len()].as_ptr().read(),
                    self.values[self.len()].as_ptr().read(),
                ))
            };
        } else {
            overflow = None;
        }
        unsafe {
            ptr::copy(
                self.keys.as_ptr().add(idx),
                self.keys.as_mut_ptr().add(idx + 1),
                self.len() - idx,
            );
            ptr::copy(
                self.values.as_ptr().add(idx),
                self.values.as_mut_ptr().add(idx + 1),
                self.len() - idx,
            );
            self.keys[idx].write(key);
            self.values[idx].write(value);
        }
        self.len += 1;
        overflow
    }

    fn insert_overflow_left(&mut self, mut idx: usize, key: K, value: V) -> (K, V) {
        debug_assert_eq!(self.len(), MAX_NUM_ELEMENTS);
        if idx == 0 {
            (key, value)
        } else {
            unsafe {
                idx -= 1;
                let overflow = (self.keys[0].as_ptr().read(), self.values[0].as_ptr().read());
                ptr::copy(self.keys.as_ptr().add(1), self.keys.as_mut_ptr(), idx);
                ptr::copy(self.values.as_ptr().add(1), self.values.as_mut_ptr(), idx);
                self.keys[idx].write(key);
                self.values[idx].write(value);
                overflow
            }
        }
    }

    fn insert_split(
        &mut self,
        alloc: &mut SlabAllocator<Self>,
        idx: usize,
        key: K,
        value: V,
    ) -> (K, V, SlabBox<Self>) {
        debug_assert_eq!(self.len(), MAX_NUM_ELEMENTS);
        unsafe {
            let mut right = Leaf::new(alloc);
            right.len = (B - 1) as _;
            match idx.cmp(&(B as usize)) {
                Ordering::Less => {
                    ptr::copy_nonoverlapping(
                        self.keys.as_ptr().add(B),
                        right.keys.as_mut_ptr(),
                        B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.values.as_ptr().add(B),
                        right.values.as_mut_ptr(),
                        B - 1,
                    );
                    let sep_key = self.keys[B - 1].as_ptr().read();
                    let sep_value = self.values[B - 1].as_ptr().read();

                    self.len = (B - 1) as _;
                    let overflow = self.insert(idx, key, value);
                    debug_assert!(overflow.is_none());
                    (sep_key, sep_value, right)
                }
                Ordering::Equal => {
                    ptr::copy_nonoverlapping(
                        self.keys.as_ptr().add(B),
                        right.keys.as_mut_ptr(),
                        B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.values.as_ptr().add(B),
                        right.values.as_mut_ptr(),
                        B - 1,
                    );
                    self.len = B as _;
                    (key, value, right)
                }
                Ordering::Greater => {
                    let sep_key = self.keys[B].as_ptr().read();
                    let sep_value = self.values[B].as_ptr().read();

                    ptr::copy_nonoverlapping(
                        self.keys.as_ptr().add(B + 1),
                        right.keys.as_mut_ptr(),
                        idx - B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.keys.as_ptr().add(idx),
                        right.keys.as_mut_ptr().add(idx - B),
                        2 * B - 1 - idx,
                    );
                    right.keys[idx - B - 1].write(key);
                    ptr::copy_nonoverlapping(
                        self.values.as_ptr().add(B + 1),
                        right.values.as_mut_ptr(),
                        idx - B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.values.as_ptr().add(idx),
                        right.values.as_mut_ptr().add(idx - B),
                        2 * B - 1 - idx,
                    );
                    right.values[idx - B - 1].write(value);

                    self.len = B as _;
                    (sep_key, sep_value, right)
                }
            }
        }
    }

    fn pop(&mut self) -> (K, V) {
        // log::info!("Leaf::shift()");

        debug_assert_ne!(self.len(), 0);

        unsafe {
            self.len -= 1;
            let key = self.keys[self.len()].as_ptr().read();
            let value = self.values[self.len()].as_ptr().read();
            (key, value)
        }
    }

    fn shift(&mut self) -> (K, V) {
        // log::info!("Leaf::shift()");

        debug_assert_ne!(self.len(), 0);

        unsafe {
            self.len -= 1;
            let key = self.keys[0].as_ptr().read();
            ptr::copy(
                self.keys.as_ptr().add(1),
                self.keys.as_mut_ptr(),
                self.len(),
            );
            let value = self.values[0].as_ptr().read();
            ptr::copy(
                self.values.as_ptr().add(1),
                self.values.as_mut_ptr(),
                self.len(),
            );
            (key, value)
        }
    }

    fn remove(&mut self, idx: usize) -> (K, V) {
        // log::info!("Leaf::remove(..)");

        debug_assert!(idx < self.len());

        unsafe {
            self.len -= 1;
            let key = self.keys[idx].as_ptr().read();
            ptr::copy(
                self.keys.as_ptr().add(idx + 1),
                self.keys.as_mut_ptr().add(idx),
                self.len() - idx,
            );
            let value = self.values[idx].as_ptr().read();
            ptr::copy(
                self.values.as_ptr().add(idx + 1),
                self.values.as_mut_ptr().add(idx),
                self.len() - idx,
            );
            (key, value)
        }
    }

    fn remove_borrow_left(&mut self, idx: usize, key: K, value: V) -> (K, V) {
        // log::info!("Leaf::remove_borrow_left(..)");

        debug_assert!(idx < self.len());

        unsafe {
            let rm_key = self.keys[idx].as_ptr().read();
            ptr::copy(self.keys.as_ptr(), self.keys.as_mut_ptr().add(1), idx);
            self.keys[0].write(key);

            let rm_value = self.values[idx].as_ptr().read();
            ptr::copy(self.values.as_ptr(), self.values.as_mut_ptr().add(1), idx);
            self.values[0].write(value);

            (rm_key, rm_value)
        }
    }

    fn merge_remove(
        &mut self,
        alloc: &mut SlabAllocator<Self>,
        sep_key: K,
        sep_value: V,
        right: SlabBox<Self>,
        idx: usize,
    ) -> (K, V) {
        // log::info!("Leaf::merge_remove(..)");

        debug_assert_eq!(self.len(), MIN_NUM_ELEMENTS);
        debug_assert_eq!(right.len(), MIN_NUM_ELEMENTS);

        unsafe {
            self.len = (2 * B - 2) as _;

            self.keys[B - 1].write(sep_key);
            let rm_key = right.keys[idx].as_ptr().read();
            ptr::copy(right.keys.as_ptr(), self.keys.as_mut_ptr().add(B), idx);
            ptr::copy(
                right.keys.as_ptr().add(idx + 1),
                self.keys.as_mut_ptr().add(B + idx),
                B - 2 - idx,
            );

            self.values[B - 1].write(sep_value);
            let rm_value = right.values[idx].as_ptr().read();
            ptr::copy(right.values.as_ptr(), self.values.as_mut_ptr().add(B), idx);
            ptr::copy(
                right.values.as_ptr().add(idx + 1),
                self.values.as_mut_ptr().add(B + idx),
                B - 2 - idx,
            );

            right.free_forget(alloc);

            (rm_key, rm_value)
        }
    }

    fn merge(
        &mut self,
        alloc: &mut SlabAllocator<Self>,
        sep_key: K,
        sep_value: V,
        right: SlabBox<Self>,
    ) {
        // log::info!("Leaf::merge(..)");

        debug_assert_eq!(self.len(), MIN_NUM_ELEMENTS - 1);
        debug_assert_eq!(right.len(), MIN_NUM_ELEMENTS);

        unsafe {
            self.len = (2 * B - 2) as _;

            self.keys[B - 2].write(sep_key);
            ptr::copy(
                right.keys.as_ptr(),
                self.keys.as_mut_ptr().add(B - 1),
                B - 1,
            );

            self.values[B - 2].write(sep_value);
            ptr::copy(
                right.values.as_ptr(),
                self.values.as_mut_ptr().add(B - 1),
                B - 1,
            );

            right.free_forget(alloc);
        }
    }
}

#[repr(align(8))]
struct Node<K, V> {
    len: u8,
    keys: [MaybeUninit<K>; MAX_NUM_ELEMENTS],
    children: [MaybeUninit<ChildUnion<K, V>>; MAX_NUM_CHILDREN],
    values: [MaybeUninit<V>; MAX_NUM_ELEMENTS],
}

impl<K, V> Child<K, V> for Node<K, V> {
    #[inline]
    fn len(&self) -> usize {
        self.len as _
    }

    #[inline]
    fn keys(&self) -> &[K] {
        unsafe { slice::from_raw_parts(self.keys.as_ptr() as _, self.len()) }
    }
    #[inline]
    fn values(&self) -> &[V] {
        unsafe { slice::from_raw_parts(self.values.as_ptr() as _, self.len()) }
    }

    #[inline]
    fn keys_mut(&mut self) -> &mut [K] {
        unsafe { slice::from_raw_parts_mut(self.keys.as_mut_ptr() as _, self.len()) }
    }
    #[inline]
    fn values_mut(&mut self) -> &mut [V] {
        unsafe { slice::from_raw_parts_mut(self.values.as_mut_ptr() as _, self.len()) }
    }

    #[inline]
    unsafe fn get_key_unchecked(&self, i: usize) -> &K {
        self.keys.get_unchecked(i).assume_init_ref()
    }
    #[inline]
    unsafe fn get_value_unchecked(&self, i: usize) -> &V {
        self.values.get_unchecked(i).assume_init_ref()
    }

    #[inline]
    unsafe fn get_key_mut_unchecked(&mut self, i: usize) -> &mut K {
        self.keys.get_unchecked_mut(i).assume_init_mut()
    }
    #[inline]
    unsafe fn get_value_mut_unchecked(&mut self, i: usize) -> &mut V {
        self.values.get_unchecked_mut(i).assume_init_mut()
    }
}

impl<K, V> Node<K, V> {
    #[inline]
    fn new(
        alloc: &mut SlabAllocator<Self>,
        key: K,
        value: V,
        lchild: ChildUnion<K, V>,
        rchild: ChildUnion<K, V>,
    ) -> SlabBox<Self> {
        unsafe {
            let mut slf = SlabBox::uninit(alloc).assume_init();
            slf.len = 1;
            slf.keys[0].write(key);
            slf.values[0].write(value);
            slf.children[0].write(lchild);
            slf.children[1].write(rchild);
            slf
        }
    }

    #[inline]
    fn children(&self) -> &[ChildUnion<K, V>] {
        unsafe { slice::from_raw_parts(self.children.as_ptr() as _, self.len() + 1) }
    }
    // #[inline]
    // fn children_mut(&mut self) -> &mut [ChildUnion<K, V>] {
    //     unsafe { slice::from_raw_parts_mut(self.children.as_mut_ptr() as _, self.len() + 1) }
    // }

    #[inline]
    unsafe fn get_child_unchecked(&self, i: usize) -> &ChildUnion<K, V> {
        self.children.get_unchecked(i).assume_init_ref()
    }
    #[inline]
    unsafe fn get_child_mut_unchecked(&mut self, i: usize) -> &mut ChildUnion<K, V> {
        self.children.get_unchecked_mut(i).assume_init_mut()
    }

    #[inline]
    fn get_all_mut(&mut self) -> (&mut [K], &mut [V], &mut [ChildUnion<K, V>]) {
        unsafe {
            (
                slice::from_raw_parts_mut(self.keys.as_mut_ptr() as _, self.len()),
                slice::from_raw_parts_mut(self.values.as_mut_ptr() as _, self.len()),
                slice::from_raw_parts_mut(self.children.as_mut_ptr() as _, self.len() + 1),
            )
        }
    }

    #[inline]
    fn push(&mut self, key: K, value: V, rchild: ChildUnion<K, V>) {
        debug_assert_ne!(self.len(), MAX_NUM_ELEMENTS);

        self.keys[self.len()].write(key);
        self.values[self.len()].write(value);
        self.len += 1;
        self.children[self.len()].write(rchild);
    }

    fn unshift(&mut self, key: K, value: V, lchild: ChildUnion<K, V>) {
        debug_assert_ne!(self.len(), MAX_NUM_ELEMENTS);

        unsafe {
            ptr::copy(
                self.keys.as_ptr(),
                self.keys.as_mut_ptr().add(1),
                self.len(),
            );
            ptr::copy(
                self.values.as_ptr(),
                self.values.as_mut_ptr().add(1),
                self.len(),
            );
            self.len += 1;
            ptr::copy(
                self.children.as_ptr(),
                self.children.as_mut_ptr().add(1),
                self.len(),
            );
            self.keys[0].write(key);
            self.values[0].write(value);
            self.children[0].write(lchild);
        }
    }

    fn insert(
        &mut self,
        idx: usize,
        key: K,
        value: V,
        rchild: ChildUnion<K, V>,
    ) -> Option<(K, V, ChildUnion<K, V>)> {
        if idx == MAX_NUM_ELEMENTS {
            return Some((key, value, rchild));
        }
        let overflow;
        if self.len() == MAX_NUM_ELEMENTS {
            self.len -= 1;
            overflow = unsafe {
                Some((
                    self.keys[self.len()].as_ptr().read(),
                    self.values[self.len()].as_ptr().read(),
                    self.children[1 + self.len()].as_ptr().read(),
                ))
            };
        } else {
            overflow = None;
        }
        unsafe {
            ptr::copy(
                self.keys.as_ptr().add(idx),
                self.keys.as_mut_ptr().add(idx + 1),
                self.len() - idx,
            );
            ptr::copy(
                self.values.as_ptr().add(idx),
                self.values.as_mut_ptr().add(idx + 1),
                self.len() - idx,
            );
            ptr::copy(
                self.children.as_ptr().add(idx + 1),
                self.children.as_mut_ptr().add(idx + 2),
                self.len() - idx,
            );
            self.keys[idx].write(key);
            self.values[idx].write(value);
            self.children[idx + 1].write(rchild);
        }
        self.len += 1;
        overflow
    }

    fn insert_overflow_left(
        &mut self,
        mut idx: usize,
        key: K,
        value: V,
        rchild: ChildUnion<K, V>,
    ) -> (K, V, ChildUnion<K, V>) {
        debug_assert_eq!(self.len(), MAX_NUM_ELEMENTS);
        if idx == 0 {
            (key, value, unsafe {
                mem::replace(self.children[0].assume_init_mut(), rchild)
            })
        } else {
            unsafe {
                idx -= 1;
                let overflow = (
                    self.keys[0].as_ptr().read(),
                    self.values[0].as_ptr().read(),
                    self.children[0].as_ptr().read(),
                );
                ptr::copy(self.keys.as_ptr().add(1), self.keys.as_mut_ptr(), idx);
                ptr::copy(self.values.as_ptr().add(1), self.values.as_mut_ptr(), idx);
                ptr::copy(
                    self.children.as_ptr().add(1),
                    self.children.as_mut_ptr(),
                    idx + 1,
                );
                self.keys[idx].write(key);
                self.values[idx].write(value);
                self.children[idx + 1].write(rchild);
                overflow
            }
        }
    }

    fn insert_split(
        &mut self,
        alloc: &mut SlabAllocator<Self>,
        idx: usize,
        key: K,
        value: V,
        rchild: ChildUnion<K, V>,
    ) -> (K, V, SlabBox<Self>) {
        debug_assert_eq!(self.len(), MAX_NUM_ELEMENTS);
        unsafe {
            let mut right = SlabBox::uninit(alloc).assume_init();
            right.len = (B - 1) as _;
            match idx.cmp(&(B as usize)) {
                Ordering::Less => {
                    ptr::copy_nonoverlapping(
                        self.keys.as_ptr().add(B),
                        right.keys.as_mut_ptr(),
                        B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.values.as_ptr().add(B),
                        right.values.as_mut_ptr(),
                        B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.children.as_ptr().add(B),
                        right.children.as_mut_ptr(),
                        B,
                    );
                    let sep_key = self.keys[B - 1].as_ptr().read();
                    let sep_value = self.values[B - 1].as_ptr().read();

                    self.len = (B - 1) as _;
                    self.insert(idx, key, value, rchild);
                    (sep_key, sep_value, right)
                }
                Ordering::Equal => {
                    ptr::copy_nonoverlapping(
                        self.keys.as_ptr().add(B),
                        right.keys.as_mut_ptr(),
                        B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.values.as_ptr().add(B),
                        right.values.as_mut_ptr(),
                        B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.children.as_ptr().add(B + 1),
                        right.children.as_mut_ptr().add(1),
                        B - 1,
                    );
                    right.children[0].write(rchild);
                    self.len = B as _;
                    (key, value, right)
                }
                Ordering::Greater => {
                    let sep_key = self.keys[B].as_ptr().read();
                    let sep_value = self.values[B].as_ptr().read();
                    // B = 4
                    // 0 1 2 3 4 5 6 7
                    // l l l l s r r
                    // l l l l s r i r
                    //           0 1 2
                    //
                    //0 1 2 3 4 5 6 7 8
                    //l l l l l r r r
                    //l l l l l r r i r

                    ptr::copy_nonoverlapping(
                        self.keys.as_ptr().add(B + 1),
                        right.keys.as_mut_ptr(),
                        idx - B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.keys.as_ptr().add(idx),
                        right.keys.as_mut_ptr().add(idx - B),
                        2 * B - 1 - idx,
                    );
                    ptr::copy_nonoverlapping(
                        self.values.as_ptr().add(B + 1),
                        right.values.as_mut_ptr(),
                        idx - B - 1,
                    );
                    ptr::copy_nonoverlapping(
                        self.values.as_ptr().add(idx),
                        right.values.as_mut_ptr().add(idx - B),
                        2 * B - 1 - idx,
                    );
                    ptr::copy_nonoverlapping(
                        self.children.as_ptr().add(B + 1),
                        right.children.as_mut_ptr(),
                        idx - B,
                    );
                    ptr::copy_nonoverlapping(
                        self.children.as_ptr().add(idx + 1),
                        right.children.as_mut_ptr().add(idx - B + 1),
                        2 * B - 1 - idx,
                    );
                    right.keys[idx - B - 1].write(key);
                    right.values[idx - B - 1].write(value);
                    right.children[idx - B].write(rchild);

                    self.len = B as _;
                    (sep_key, sep_value, right)
                }
            }
        }
    }

    fn pop(&mut self) -> (K, V, ChildUnion<K, V>) {
        // log::info!("Node::pop()");

        debug_assert_ne!(self.len(), 0);

        unsafe {
            let rchild = self.children[self.len()].as_ptr().read();
            self.len -= 1;
            let key = self.keys[self.len()].as_ptr().read();
            let value = self.values[self.len()].as_ptr().read();
            (key, value, rchild)
        }
    }

    fn shift(&mut self) -> (K, V, ChildUnion<K, V>) {
        // log::info!("Node::shift(..)");

        debug_assert_ne!(self.len(), 0);

        unsafe {
            let lchild = self.children[0].as_ptr().read();
            ptr::copy(
                self.children.as_ptr().add(1),
                self.children.as_mut_ptr(),
                self.len(),
            );
            self.len -= 1;
            let key = self.keys[0].as_ptr().read();
            ptr::copy(
                self.keys.as_ptr().add(1),
                self.keys.as_mut_ptr(),
                self.len(),
            );
            let value = self.values[0].as_ptr().read();
            ptr::copy(
                self.values.as_ptr().add(1),
                self.values.as_mut_ptr(),
                self.len(),
            );
            (key, value, lchild)
        }
    }

    fn remove(&mut self, idx: usize) -> (K, V, ChildUnion<K, V>) {
        // log::info!("Node::remove(..)");
        debug_assert!(idx < self.len());

        unsafe {
            self.len -= 1;
            let key = self.keys[idx].as_ptr().read();
            ptr::copy(
                self.keys.as_ptr().add(idx + 1),
                self.keys.as_mut_ptr().add(idx),
                self.len() - idx,
            );
            let value = self.values[idx].as_ptr().read();
            ptr::copy(
                self.values.as_ptr().add(idx + 1),
                self.values.as_mut_ptr().add(idx),
                self.len() - idx,
            );
            let rchild = self.children[idx + 1].as_ptr().read();
            ptr::copy(
                self.children.as_ptr().add(idx + 2),
                self.children.as_mut_ptr().add(idx + 1),
                self.len() - idx,
            );
            (key, value, rchild)
        }
    }

    fn remove_borrow_left(
        &mut self,
        idx: usize,
        key: K,
        value: V,
        lchild: ChildUnion<K, V>,
    ) -> (K, V, ChildUnion<K, V>) {
        // log::info!("Node::remove_borrow_left(..)");

        debug_assert!(idx < self.len());

        unsafe {
            let rm_key = self.keys[idx].as_ptr().read();
            ptr::copy(self.keys.as_ptr(), self.keys.as_mut_ptr().add(1), idx);
            self.keys[0].write(key);

            let rm_value = self.values[idx].as_ptr().read();
            ptr::copy(self.values.as_ptr(), self.values.as_mut_ptr().add(1), idx);
            self.values[0].write(value);

            let rm_rchild = self.children[idx + 1].as_ptr().read();
            ptr::copy(
                self.children.as_ptr(),
                self.children.as_mut_ptr().add(1),
                idx + 1,
            );
            self.children[0].write(lchild);

            (rm_key, rm_value, rm_rchild)
        }
    }

    fn merge_remove(
        &mut self,
        alloc: &mut SlabAllocator<Self>,
        sep_key: K,
        sep_value: V,
        right: SlabBox<Self>,
        idx: usize,
    ) -> (K, V, ChildUnion<K, V>) {
        // log::info!("Node::merge_remove(..)");

        debug_assert_eq!(self.len(), MIN_NUM_ELEMENTS);
        debug_assert_eq!(right.len(), MIN_NUM_ELEMENTS);

        unsafe {
            self.len = (2 * B - 2) as _;

            self.keys[B - 1].write(sep_key);
            let rm_key = right.keys[idx].as_ptr().read();
            ptr::copy(right.keys.as_ptr(), self.keys.as_mut_ptr().add(B), idx);
            ptr::copy(
                right.keys.as_ptr().add(idx + 1),
                self.keys.as_mut_ptr().add(B + idx),
                B - 2 - idx,
            );

            self.values[B - 1].write(sep_value);
            let rm_value = right.values[idx].as_ptr().read();
            ptr::copy(right.values.as_ptr(), self.values.as_mut_ptr().add(B), idx);
            ptr::copy(
                right.values.as_ptr().add(idx + 1),
                self.values.as_mut_ptr().add(B + idx),
                B - 2 - idx,
            );

            let rm_rchild = right.children[idx + 1].as_ptr().read();
            ptr::copy(
                right.children.as_ptr(),
                self.children.as_mut_ptr().add(B),
                idx + 1,
            );
            ptr::copy(
                right.children.as_ptr().add(idx + 2),
                self.children.as_mut_ptr().add(B + idx + 1),
                B - 2 - idx,
            );

            right.free_forget(alloc);

            (rm_key, rm_value, rm_rchild)
        }
    }

    fn merge(
        &mut self,
        alloc: &mut SlabAllocator<Self>,
        sep_key: K,
        sep_value: V,
        right: SlabBox<Self>,
    ) {
        // log::info!("Node::merge(..)");
        debug_assert_eq!(self.len(), MIN_NUM_ELEMENTS - 1);
        debug_assert_eq!(right.len(), MIN_NUM_ELEMENTS);

        unsafe {
            self.len = (2 * B - 2) as _;

            self.keys[B - 2].write(sep_key);
            ptr::copy(
                right.keys.as_ptr(),
                self.keys.as_mut_ptr().add(B - 1),
                B - 1,
            );

            self.values[B - 2].write(sep_value);
            ptr::copy(
                right.values.as_ptr(),
                self.values.as_mut_ptr().add(B - 1),
                B - 1,
            );

            ptr::copy(
                right.children.as_ptr(),
                self.children.as_mut_ptr().add(B - 1),
                B,
            );

            right.free_forget(alloc);
        }
    }
}

#[cfg(not(debug_assertions))]
union ChildUnion<K, V> {
    node: ManuallyDrop<SlabBox<Node<K, V>>>,
    leaf: ManuallyDrop<SlabBox<Leaf<K, V>>>,
}

#[cfg(debug_assertions)]
enum ChildUnion<K, V> {
    Node(SlabBox<Node<K, V>>),
    Leaf(SlabBox<Leaf<K, V>>),
}

#[cfg(not(debug_assertions))]
impl<K, V> ChildUnion<K, V> {
    #[inline]
    unsafe fn into_leaf(self) -> SlabBox<Leaf<K, V>> {
        let md = ManuallyDrop::new(self);
        ptr::read(&*md.leaf as *const _)
    }

    #[inline]
    unsafe fn into_node(self) -> SlabBox<Node<K, V>> {
        let md = ManuallyDrop::new(self);
        ptr::read(&*md.node as *const _)
    }

    #[inline]
    fn leaf(leaf: SlabBox<Leaf<K, V>>) -> Self {
        Self {
            leaf: ManuallyDrop::new(leaf),
        }
    }

    #[cfg(not(debug_assertions))]
    #[inline]
    fn node(node: SlabBox<Node<K, V>>) -> Self {
        Self {
            node: ManuallyDrop::new(node),
        }
    }

    #[inline]
    unsafe fn as_leaf(&self) -> &Leaf<K, V> {
        &*self.leaf
    }

    #[inline]
    unsafe fn as_node(&self) -> &Node<K, V> {
        &*self.node
    }

    #[inline]
    unsafe fn as_leaf_mut(&mut self) -> &mut Leaf<K, V> {
        &mut *self.leaf
    }

    #[inline]
    unsafe fn as_node_mut(&mut self) -> &mut Node<K, V> {
        &mut *self.node
    }
}

#[cfg(debug_assertions)]
impl<K, V> ChildUnion<K, V> {
    #[inline]
    unsafe fn into_leaf(self) -> SlabBox<Leaf<K, V>> {
        let md = ManuallyDrop::new(self);
        match &*md {
            Self::Leaf(leaf) => ptr::read(leaf),
            Self::Node(_) => unreachable!(),
        }
    }

    #[inline]
    unsafe fn into_node(self) -> SlabBox<Node<K, V>> {
        let md = ManuallyDrop::new(self);
        match &*md {
            Self::Leaf(_) => unreachable!(),
            Self::Node(node) => ptr::read(node),
        }
    }

    #[inline]
    fn leaf(leaf: SlabBox<Leaf<K, V>>) -> Self {
        Self::Leaf(leaf)
    }

    #[inline]
    fn node(node: SlabBox<Node<K, V>>) -> Self {
        Self::Node(node)
    }

    #[inline]
    unsafe fn as_leaf(&self) -> &Leaf<K, V> {
        match self {
            Self::Leaf(leaf) => &*leaf,
            Self::Node(_node) => unreachable!(),
        }
    }

    #[inline]
    unsafe fn as_node(&self) -> &Node<K, V> {
        match self {
            Self::Leaf(_leaf) => unreachable!(),
            Self::Node(node) => &*node,
        }
    }

    #[inline]
    unsafe fn as_leaf_mut(&mut self) -> &mut Leaf<K, V> {
        match self {
            Self::Leaf(leaf) => &mut *leaf,
            Self::Node(_node) => unreachable!(),
        }
    }

    #[inline]
    unsafe fn as_node_mut(&mut self) -> &mut Node<K, V> {
        match self {
            Self::Leaf(_leaf) => unreachable!(),
            Self::Node(node) => &mut *node,
        }
    }
}

impl<K, V> Drop for ChildUnion<K, V> {
    fn drop(&mut self) {
        panic!("Dropped undropable type: `{}`", type_name::<Self>(),);
    }
}

impl<K, V> Drop for Node<K, V> {
    fn drop(&mut self) {
        panic!("Dropped undropable type: `{}`", type_name::<Self>(),);
    }
}

impl<K, V> Drop for Leaf<K, V> {
    fn drop(&mut self) {
        panic!("Dropped undropable type: `{}`", type_name::<Self>(),);
    }
}

pub struct BTree<K, V> {
    root: MaybeUninit<ChildUnion<K, V>>,
    depth: u8,
    size: usize,

    node_alloc: SlabAllocator<Node<K, V>>,
    leaf_alloc: SlabAllocator<Leaf<K, V>>,
}

impl<K, V> BTree<K, V> {
    pub fn new(chunk: &'static mut [u8]) -> Self {
        assert!(8 * size_of::<Node<K, V>>() < chunk.len());
        let (node_alloc_chunk, leaf_alloc_chunk) = chunk.split_at_mut(
            chunk.len() * size_of::<Node<K, V>>()
                / (size_of::<Node<K, V>>() + (B - 1) * size_of::<Leaf<K, V>>()),
        );

        let node_alloc = SlabAllocator::new(node_alloc_chunk);
        let leaf_alloc = SlabAllocator::new(leaf_alloc_chunk);

        Self {
            root: MaybeUninit::uninit(),
            depth: 0,
            size: 0,
            node_alloc,
            leaf_alloc,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn needs_new_chunk(&self) -> bool {
        self.leaf_alloc.needs_new_chunk() || self.node_alloc.needs_new_chunk()
    }

    #[inline]
    pub fn add_chunk(&mut self, chunk: &'static mut [u8]) {
        let (node_alloc_chunk, leaf_alloc_chunk) = chunk.split_at_mut(
            chunk.len() * size_of::<Node<K, V>>()
                / (size_of::<Node<K, V>>() + (B - 1) * size_of::<Leaf<K, V>>()),
        );
        self.leaf_alloc.add_chunk(leaf_alloc_chunk);
        self.node_alloc.add_chunk(node_alloc_chunk);
    }

    pub fn get_entry<Q>(&self, key: &Q) -> Option<(&K, &V)>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        match self.depth {
            0 => None,
            1 => {
                let root = unsafe { self.root.assume_init_ref().as_leaf() };
                match root.linsearch(key) {
                    Ok(i) => unsafe {
                        Some((root.get_key_unchecked(i), root.get_value_unchecked(i)))
                    },
                    Err(_) => None,
                }
            }
            _ => {
                let mut node = unsafe { self.root.assume_init_ref().as_node() };
                for _ in 0..self.depth - 2 {
                    match node.linsearch(key) {
                        Ok(i) => {
                            return unsafe {
                                Some((node.get_key_unchecked(i), node.get_value_unchecked(i)))
                            };
                        }
                        Err(i) => {
                            node = unsafe { node.get_child_unchecked(i).as_node() };
                        }
                    }
                }
                match node.linsearch(key) {
                    Ok(i) => unsafe {
                        Some((node.get_key_unchecked(i), node.get_value_unchecked(i)))
                    },
                    Err(i) => {
                        let leaf = unsafe { node.get_child_unchecked(i).as_leaf() };
                        match leaf.linsearch(key) {
                            Ok(i) => unsafe {
                                Some((leaf.get_key_unchecked(i), leaf.get_value_unchecked(i)))
                            },
                            Err(_) => None,
                        }
                    }
                }
            }
        }
    }

    pub fn get_entry_mut<Q>(&mut self, key: &Q) -> Option<(&K, &mut V)>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        match self.depth {
            0 => None,
            1 => {
                let root = unsafe { self.root.assume_init_mut().as_leaf_mut() };
                match root.linsearch(key) {
                    Ok(i) => unsafe {
                        let (keys, values) = root.get_all_mut();
                        Some((keys.get_unchecked(i), values.get_unchecked_mut(i)))
                    },
                    Err(_) => None,
                }
            }
            _ => {
                let mut node = unsafe { self.root.assume_init_mut().as_node_mut() };
                for _ in 0..self.depth - 2 {
                    match node.linsearch(key) {
                        Ok(i) => unsafe {
                            let (keys, values, _) = node.get_all_mut();
                            return Some((keys.get_unchecked(i), values.get_unchecked_mut(i)));
                        },
                        Err(i) => {
                            node = unsafe { node.get_child_mut_unchecked(i).as_node_mut() };
                        }
                    }
                }
                match node.linsearch(key) {
                    Ok(i) => unsafe {
                        let (keys, values, _) = node.get_all_mut();
                        Some((keys.get_unchecked(i), values.get_unchecked_mut(i)))
                    },
                    Err(i) => {
                        let leaf = unsafe { node.get_child_mut_unchecked(i).as_leaf_mut() };
                        match leaf.linsearch(key) {
                            Ok(i) => unsafe {
                                let (keys, values) = leaf.get_all_mut();
                                Some((keys.get_unchecked(i), values.get_unchecked_mut(i)))
                            },
                            Err(_) => None,
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        Some(self.get_entry(key)?.1)
    }

    #[inline]
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        Some(self.get_entry_mut(key)?.1)
    }

    pub fn insert(&mut self, mut key: K, mut value: V) -> Option<(K, V)>
    where
        K: Ord,
    {
        match self.depth {
            0 => {
                let mut leaf = Leaf::new(&mut self.leaf_alloc);
                leaf.push(key, value);
                self.root.write(ChildUnion::leaf(leaf));
                self.depth = 1;
                self.size = 1;

                None
            }
            1 => {
                let root = unsafe { self.root.assume_init_mut().as_leaf_mut() };
                match root.linsearch(&key) {
                    Ok(i) => unsafe {
                        Some((
                            mem::replace(root.get_key_mut_unchecked(i), key),
                            mem::replace(root.get_value_mut_unchecked(i), value),
                        ))
                    },
                    Err(i) => {
                        self.size += 1;
                        if root.len() < MAX_NUM_ELEMENTS {
                            let overflow = root.insert(i, key, value);
                            assert!(overflow.is_none());
                        } else {
                            self.depth = 2;

                            let (sep_key, sep_value, right) =
                                root.insert_split(&mut self.leaf_alloc, i, key, value);
                            let left = unsafe { self.root.as_ptr().read() };

                            self.root.write(ChildUnion::node(Node::new(
                                &mut self.node_alloc,
                                sep_key,
                                sep_value,
                                left,
                                ChildUnion::leaf(right),
                            )));
                        }
                        None
                    }
                }
            }
            _ => {
                let mut nodes_stack = RefStack::<_, 24>::with_root(unsafe {
                    self.root.assume_init_mut().as_node_mut()
                });
                let mut indices = [0; 24];

                for depth in 0..self.depth as usize - 2 {
                    let node = nodes_stack.top_mut().unwrap();
                    match node.linsearch(&key) {
                        Ok(i) => unsafe {
                            return Some((
                                mem::replace(node.get_key_mut_unchecked(i), key),
                                mem::replace(node.get_value_mut_unchecked(i), value),
                            ));
                        },
                        Err(i) => {
                            indices[depth] = i;
                            nodes_stack.push(|node| unsafe {
                                node.get_child_mut_unchecked(i).as_node_mut()
                            });
                        }
                    }
                }

                let (mut sep_key, mut sep_value, mut right);
                let node: &mut Node<K, V> = nodes_stack.top_mut().unwrap();
                match node.linsearch(&key) {
                    Ok(i) => unsafe {
                        return Some((
                            mem::replace(node.get_key_mut_unchecked(i), key),
                            mem::replace(node.get_value_mut_unchecked(i), value),
                        ));
                    },
                    Err(i) => {
                        let j;
                        indices[self.depth as usize - 2] = i;
                        let leaf = unsafe { node.get_child_mut_unchecked(i).as_leaf_mut() };
                        match leaf.linsearch(&key) {
                            Ok(i) => unsafe {
                                return Some((
                                    mem::replace(leaf.get_key_mut_unchecked(i), key),
                                    mem::replace(leaf.get_value_mut_unchecked(i), value),
                                ));
                            },
                            Err(i) => {
                                j = i;
                                indices[self.depth as usize - 1] = j;
                                self.size += 1;
                                if leaf.len() < MAX_NUM_ELEMENTS {
                                    let overflow = leaf.insert(j, key, value);
                                    assert!(overflow.is_none());
                                    return None;
                                }
                            }
                        }

                        if 0 < i {
                            let left_neighbour =
                                unsafe { node.get_child_unchecked(i - 1).as_leaf() };
                            if left_neighbour.len() < MAX_NUM_ELEMENTS {
                                let child =
                                    unsafe { node.get_child_mut_unchecked(i).as_leaf_mut() };
                                (key, value) = child.insert_overflow_left(j, key, value);
                                key =
                                    mem::replace(unsafe { node.get_key_mut_unchecked(i - 1) }, key);
                                value = mem::replace(
                                    unsafe { node.get_value_mut_unchecked(i - 1) },
                                    value,
                                );
                                let leaf =
                                    unsafe { node.get_child_mut_unchecked(i - 1).as_leaf_mut() };
                                leaf.push(key, value);
                                return None;
                            }
                        }
                        if i < node.len() {
                            let right_neighbour =
                                unsafe { node.get_child_unchecked(i + 1).as_leaf() };
                            if right_neighbour.len() < MAX_NUM_ELEMENTS {
                                let child =
                                    unsafe { node.get_child_mut_unchecked(i).as_leaf_mut() };
                                (key, value) = child.insert(j, key, value).unwrap();
                                key = mem::replace(unsafe { node.get_key_mut_unchecked(i) }, key);
                                value =
                                    mem::replace(unsafe { node.get_value_mut_unchecked(i) }, value);
                                let leaf =
                                    unsafe { node.get_child_mut_unchecked(i + 1).as_leaf_mut() };
                                leaf.unshift(key, value);
                                return None;
                            }
                        }
                        let leaf_right;
                        let child = unsafe { node.get_child_mut_unchecked(i).as_leaf_mut() };
                        (sep_key, sep_value, leaf_right) =
                            child.insert_split(&mut self.leaf_alloc, j, key, value);
                        right = ChildUnion::leaf(leaf_right);
                        if node.len() < MAX_NUM_ELEMENTS {
                            let overflow = node.insert(i, sep_key, sep_value, right);
                            debug_assert!(overflow.is_none());
                            return None;
                        }
                    }
                }

                while 1 < nodes_stack.len() {
                    nodes_stack.pop();
                    let depth = nodes_stack.len() - 1;
                    let node: &mut Node<K, V> = nodes_stack.top_mut().unwrap();
                    let i = indices[depth];
                    let j = indices[depth + 1];

                    if 0 < i {
                        let left_neighbour = unsafe { node.get_child_unchecked(i - 1).as_node() };
                        if left_neighbour.len() < MAX_NUM_ELEMENTS {
                            let child = unsafe { node.get_child_mut_unchecked(i).as_node_mut() };
                            let (mut key, mut value, lchild) =
                                child.insert_overflow_left(j, sep_key, sep_value, right);

                            key = mem::replace(unsafe { node.get_key_mut_unchecked(i - 1) }, key);
                            value =
                                mem::replace(unsafe { node.get_value_mut_unchecked(i - 1) }, value);

                            let neighbour =
                                unsafe { node.get_child_mut_unchecked(i - 1).as_node_mut() };
                            neighbour.push(key, value, lchild);
                            return None;
                        }
                    }
                    if i < node.len() {
                        let right_neighbour = unsafe { node.get_child_unchecked(i + 1).as_node() };
                        if right_neighbour.len() < MAX_NUM_ELEMENTS {
                            let child = unsafe { node.get_child_mut_unchecked(i).as_node_mut() };
                            let (mut key, mut value, rchild) =
                                child.insert(j, sep_key, sep_value, right).unwrap();
                            key = mem::replace(unsafe { node.get_key_mut_unchecked(i) }, key);
                            value = mem::replace(unsafe { node.get_value_mut_unchecked(i) }, value);
                            let node = unsafe { node.get_child_mut_unchecked(i + 1).as_node_mut() };
                            node.unshift(key, value, rchild);
                            return None;
                        }
                    }
                    let node_right;
                    let child = unsafe { node.get_child_mut_unchecked(i).as_node_mut() };
                    (sep_key, sep_value, node_right) =
                        child.insert_split(&mut self.node_alloc, j, sep_key, sep_value, right);
                    right = ChildUnion::node(node_right);
                    if node.len() < MAX_NUM_ELEMENTS {
                        let overflow = node.insert(i, sep_key, sep_value, right);
                        debug_assert!(overflow.is_none());
                        return None;
                    }
                }
                let root = nodes_stack.pop().unwrap();
                let node_right;
                (sep_key, sep_value, node_right) =
                    root.insert_split(&mut self.node_alloc, indices[0], sep_key, sep_value, right);
                right = ChildUnion::node(node_right);
                let new_root = Node::new(
                    &mut self.node_alloc,
                    sep_key,
                    sep_value,
                    unsafe { self.root.as_ptr().read() },
                    right,
                );
                self.root.write(ChildUnion::node(new_root));
                self.depth += 1;
                None
            }
        }
    }

    #[inline]
    pub fn remove<Q>(&mut self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        match self.depth {
            0 => None,
            1 => {
                let root = unsafe { self.root.assume_init_mut().as_leaf_mut() };
                match root.linsearch(&key) {
                    Ok(i) => {
                        self.size -= 1;
                        let (key, value) = root.remove(i);
                        if root.len() == 0 {
                            unsafe {
                                self.depth = 0;
                                self.root
                                    .as_ptr()
                                    .read()
                                    .into_leaf()
                                    .free_forget(&mut self.leaf_alloc);
                            }
                        }
                        Some((key, value))
                    }
                    Err(_) => None,
                }
            }
            _ => {
                let mut node_stack = RefStack::<_, 24>::with_root(unsafe {
                    self.root.assume_init_mut().as_node_mut()
                });
                let mut indices = [0; 24];
                let mut target_depth = usize::MAX;

                for depth in 0..self.depth as usize - 2 {
                    let node = node_stack.top().unwrap();
                    let i = if target_depth == usize::MAX {
                        match node.linsearch(key) {
                            Ok(i) => {
                                self.size -= 1;
                                target_depth = depth;
                                i
                            }
                            Err(i) => i,
                        }
                    } else {
                        node.len()
                    };
                    indices[depth] = i;
                    node_stack
                        .push(|node| unsafe { node.get_child_mut_unchecked(i).as_node_mut() });
                }

                let depth = self.depth as usize - 2;
                let node: &mut Node<K, V> = node_stack.top_mut().unwrap();
                let i = if target_depth == usize::MAX {
                    match node.linsearch(key) {
                        Ok(i) => {
                            self.size -= 1;
                            target_depth = depth;
                            i
                        }
                        Err(i) => i,
                    }
                } else {
                    node.len()
                };
                indices[depth] = i;
                let leaf = unsafe { node.get_child_mut_unchecked(i).as_leaf_mut() };

                let j = {
                    let depth = self.depth as usize - 1;
                    let i = if target_depth == usize::MAX {
                        match leaf.linsearch(key) {
                            Ok(i) => {
                                self.size -= 1;
                                target_depth = depth;
                                i
                            }
                            Err(_) => {
                                return None;
                            }
                        }
                    } else {
                        leaf.len() - 1
                    };

                    if MIN_NUM_ELEMENTS < leaf.len() {
                        let (key, value) = leaf.remove(i);
                        if depth == target_depth {
                            return Some((key, value));
                        }
                        while target_depth < node_stack.len() - 1 {
                            let root = node_stack.pop();
                            debug_assert!(root.is_none());
                        }
                        let node: &mut Node<K, V> = node_stack.top_mut().unwrap();
                        let i = indices[target_depth];
                        return Some((
                            mem::replace(unsafe { node.get_key_mut_unchecked(i) }, key),
                            mem::replace(unsafe { node.get_value_mut_unchecked(i) }, value),
                        ));
                    }

                    i
                };

                // log::info!("target_depth: {target_depth}, highest_node_depth: {depth}");

                if target_depth == depth {
                    let (keys, values, children) = node.get_all_mut();
                    let child = unsafe { children[i].as_leaf_mut() };
                    mem::swap(unsafe { child.get_key_mut_unchecked(j) }, &mut keys[i]);
                    mem::swap(unsafe { child.get_value_mut_unchecked(j) }, &mut values[i]);
                }

                if 0 < i {
                    let left_neighbour =
                        unsafe { node.get_child_mut_unchecked(i - 1).as_leaf_mut() };
                    if MIN_NUM_ELEMENTS < left_neighbour.len() {
                        let (mut key, mut value) = left_neighbour.pop();
                        key = mem::replace(unsafe { node.get_key_mut_unchecked(i - 1) }, key);
                        value = mem::replace(unsafe { node.get_value_mut_unchecked(i - 1) }, value);
                        let child = unsafe { node.get_child_mut_unchecked(i).as_leaf_mut() };
                        (key, value) = child.remove_borrow_left(j, key, value);

                        if depth <= target_depth {
                            return Some((key, value));
                        } else {
                            while target_depth < node_stack.len() - 1 {
                                let root = node_stack.pop();
                                debug_assert!(root.is_none());
                            }
                            let node: &mut Node<K, V> = node_stack.top_mut().unwrap();
                            let i = indices[target_depth];
                            return Some((
                                mem::replace(unsafe { node.get_key_mut_unchecked(i) }, key),
                                mem::replace(unsafe { node.get_value_mut_unchecked(i) }, value),
                            ));
                        }
                    }
                }
                if i < node.len() {
                    let right_neighbour =
                        unsafe { node.get_child_mut_unchecked(i + 1).as_leaf_mut() };
                    if MIN_NUM_ELEMENTS < right_neighbour.len() {
                        let (mut key, mut value) = right_neighbour.shift();
                        key = mem::replace(unsafe { node.get_key_mut_unchecked(i) }, key);
                        value = mem::replace(unsafe { node.get_value_mut_unchecked(i) }, value);
                        let child = unsafe { node.get_child_mut_unchecked(i).as_leaf_mut() };
                        let (rm_key, rm_value) = child.remove(j);
                        child.push(key, value);

                        if depth <= target_depth {
                            return Some((rm_key, rm_value));
                        } else {
                            while target_depth < node_stack.len() - 1 {
                                let root = node_stack.pop();
                                debug_assert!(root.is_none());
                            }
                            let node: &mut Node<K, V> = node_stack.top_mut().unwrap();
                            let i = indices[target_depth];
                            return Some((
                                mem::replace(unsafe { node.get_key_mut_unchecked(i) }, rm_key),
                                mem::replace(unsafe { node.get_value_mut_unchecked(i) }, rm_value),
                            ));
                        }
                    }
                }

                let (mut rm_key, mut rm_value);
                let mut hole;

                if 0 < i {
                    hole = i - 1;
                    let sep_key = unsafe { node.keys[i - 1].as_ptr().read() };
                    let sep_value = unsafe { node.values[i - 1].as_ptr().read() };
                    let child = unsafe { node.children[i].as_ptr().read().into_leaf() };

                    let left = unsafe { node.get_child_mut_unchecked(i - 1).as_leaf_mut() };
                    (rm_key, rm_value) =
                        left.merge_remove(&mut self.leaf_alloc, sep_key, sep_value, child, j);
                } else {
                    hole = 0;
                    let sep_key = unsafe { node.keys[0].as_ptr().read() };
                    let sep_value = unsafe { node.values[0].as_ptr().read() };
                    let right = unsafe { node.children[1].as_ptr().read().into_leaf() };

                    let child = unsafe { node.get_child_mut_unchecked(0).as_leaf_mut() };
                    (rm_key, rm_value) = child.remove(j);
                    child.merge(&mut self.leaf_alloc, sep_key, sep_value, right);
                }

                if MIN_NUM_ELEMENTS < node.len() {
                    mem::forget(node.remove(hole));

                    if depth <= target_depth {
                        return Some((rm_key, rm_value));
                    } else {
                        while target_depth < node_stack.len() - 1 {
                            let root = node_stack.pop();
                            debug_assert!(root.is_none());
                        }
                        let node: &mut Node<K, V> = node_stack.top_mut().unwrap();
                        let i = indices[target_depth];
                        return Some((
                            mem::replace(unsafe { node.get_key_mut_unchecked(i) }, rm_key),
                            mem::replace(unsafe { node.get_value_mut_unchecked(i) }, rm_value),
                        ));
                    }
                }

                while 1 < node_stack.len() {
                    node_stack.pop();

                    let depth = node_stack.len() - 1;
                    let i = indices[depth];

                    let node: &mut Node<K, V> = node_stack.top_mut().unwrap();

                    if depth == target_depth {
                        rm_key = mem::replace(unsafe { node.get_key_mut_unchecked(i) }, rm_key);
                        rm_value =
                            mem::replace(unsafe { node.get_value_mut_unchecked(i) }, rm_value);
                    }

                    if 0 < i {
                        let left_neighbour =
                            unsafe { node.get_child_mut_unchecked(i - 1).as_node_mut() };
                        if MIN_NUM_ELEMENTS < left_neighbour.len() {
                            let (mut key, mut value, rchild) = left_neighbour.pop();
                            key = mem::replace(unsafe { node.get_key_mut_unchecked(i - 1) }, key);
                            value =
                                mem::replace(unsafe { node.get_value_mut_unchecked(i - 1) }, value);
                            let child = unsafe { node.get_child_mut_unchecked(i).as_node_mut() };
                            mem::forget(child.remove_borrow_left(hole, key, value, rchild));

                            if depth <= target_depth {
                                return Some((rm_key, rm_value));
                            } else {
                                while target_depth < node_stack.len() - 1 {
                                    let root = node_stack.pop();
                                    debug_assert!(root.is_none());
                                }
                                let node: &mut Node<K, V> = node_stack.top_mut().unwrap();
                                let i = indices[target_depth];
                                return Some((
                                    mem::replace(unsafe { node.get_key_mut_unchecked(i) }, rm_key),
                                    mem::replace(
                                        unsafe { node.get_value_mut_unchecked(i) },
                                        rm_value,
                                    ),
                                ));
                            }
                        }
                    }
                    if i < node.len() {
                        let right_neighbour =
                            unsafe { node.get_child_mut_unchecked(i + 1).as_node_mut() };
                        if MIN_NUM_ELEMENTS < right_neighbour.len() {
                            let (mut key, mut value, lchild) = right_neighbour.shift();
                            key = mem::replace(unsafe { node.get_key_mut_unchecked(i) }, key);
                            value = mem::replace(unsafe { node.get_value_mut_unchecked(i) }, value);
                            let child = unsafe { node.get_child_mut_unchecked(i).as_node_mut() };
                            mem::forget(child.remove(hole));
                            child.push(key, value, lchild);

                            if depth <= target_depth {
                                return Some((rm_key, rm_value));
                            } else {
                                while target_depth < node_stack.len() - 1 {
                                    let root = node_stack.pop();
                                    debug_assert!(root.is_none());
                                }
                                let node: &mut Node<K, V> = node_stack.top_mut().unwrap();
                                let i = indices[target_depth];
                                return Some((
                                    mem::replace(unsafe { node.get_key_mut_unchecked(i) }, rm_key),
                                    mem::replace(
                                        unsafe { node.get_value_mut_unchecked(i) },
                                        rm_value,
                                    ),
                                ));
                            }
                        }
                    }

                    if 0 < i {
                        let sep_key = unsafe { node.keys[i - 1].as_ptr().read() };
                        let sep_value = unsafe { node.values[i - 1].as_ptr().read() };
                        let child = unsafe { node.children[i].as_ptr().read().into_node() };

                        let left = unsafe { node.get_child_mut_unchecked(i - 1).as_node_mut() };
                        mem::forget(left.merge_remove(
                            &mut self.node_alloc,
                            sep_key,
                            sep_value,
                            child,
                            hole,
                        ));
                        hole = i - 1;
                    } else {
                        let sep_key = unsafe { node.keys[0].as_ptr().read() };
                        let sep_value = unsafe { node.values[0].as_ptr().read() };
                        let right = unsafe { node.children[1].as_ptr().read().into_node() };

                        let child = unsafe { node.get_child_mut_unchecked(0).as_node_mut() };
                        mem::forget(child.remove(hole));
                        child.merge(&mut self.node_alloc, sep_key, sep_value, right);
                        hole = 0;
                    }

                    if MIN_NUM_ELEMENTS < node.len() {
                        mem::forget(node.remove(hole));

                        if depth <= target_depth {
                            return Some((rm_key, rm_value));
                        } else {
                            while target_depth < node_stack.len() - 1 {
                                let root = node_stack.pop();
                                debug_assert!(root.is_none());
                            }
                            let node: &mut Node<K, V> = node_stack.top_mut().unwrap();
                            let i = indices[target_depth];
                            return Some((
                                mem::replace(unsafe { node.get_key_mut_unchecked(i) }, rm_key),
                                mem::replace(unsafe { node.get_value_mut_unchecked(i) }, rm_value),
                            ));
                        }
                    }
                }

                let root: &mut Node<K, V> = node_stack.pop().unwrap();
                mem::forget(root.remove(hole));
                if root.len() == 0 {
                    self.depth -= 1;
                    let root = unsafe { self.root.as_ptr().read().into_node() };
                    self.root.write(unsafe { root.children[0].as_ptr().read() });
                    root.free_forget(&mut self.node_alloc);
                }
                Some((rm_key, rm_value))
            }
        }
    }
}

struct KVPair<K, V> {
    key: K,
    value: V,
}

impl<K, V> KVPair<K, V> {
    #[inline]
    const fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for KVPair<K, V> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} => {:?}", self.key, self.value)
    }
}

#[cfg(debug_assertions)]
impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for ChildUnion<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChildUnion::Node(node) => node.fmt(f),
            ChildUnion::Leaf(leaf) => leaf.fmt(f),
        }
    }
}

#[cfg(debug_assertions)]
impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for Node<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node(len={}, ", self.len())?;
        let mut dbg_list = f.debug_list();
        dbg_list.entry(&self.children()[0]);
        for i in 0..self.len() {
            dbg_list.entry(&KVPair::new(&self.keys()[i], &self.values()[i]));
            dbg_list.entry(&self.children()[i + 1]);
        }
        dbg_list.finish()?;
        write!(f, ")")?;
        Ok(())
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for Leaf<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Leaf(len={}, ", self.len())?;
        let mut dbg_list = f.debug_list();
        for (key, value) in self.keys().iter().zip(self.values().iter()) {
            dbg_list.entry(&KVPair::new(key, value));
        }
        dbg_list.finish()?;
        write!(f, ")")?;
        Ok(())
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for BTree<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugNode<'a, K, V> {
            node: &'a Node<K, V>,
            depth: u8,
        }

        impl<'a, K, V> core::ops::Deref for DebugNode<'a, K, V> {
            type Target = Node<K, V>;
            fn deref(&self) -> &Self::Target {
                self.node
            }
        }

        impl<'a, K: fmt::Debug, V: fmt::Debug> fmt::Debug for DebugNode<'a, K, V> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let add_child = |dbg_list: &mut fmt::DebugList, i: usize| match self.depth {
                    0 | 1 => unreachable!(),
                    2 => unsafe {
                        dbg_list.entry(self.children()[i].as_leaf());
                    },
                    _ => unsafe {
                        dbg_list.entry(&DebugNode {
                            depth: self.depth - 1,
                            node: self.children()[i].as_node(),
                        });
                    },
                };

                write!(f, "Node(len={}, ", self.len())?;
                let mut dbg_list = f.debug_list();
                add_child(&mut dbg_list, 0);
                for i in 0..self.len() {
                    dbg_list.entry(&KVPair::new(&self.keys()[i], &self.values()[i]));
                    add_child(&mut dbg_list, i + 1);
                }
                dbg_list.finish()?;
                write!(f, ")")?;
                Ok(())
            }
        }

        let mut dbg_struct = f.debug_struct("BTree");
        dbg_struct.field("size", &self.size);
        dbg_struct.field("depth", &self.depth);
        match self.depth {
            0 => {
                dbg_struct.field("root", &None::<()>);
            }
            1 => {
                dbg_struct.field("root", unsafe {
                    &Some(self.root.assume_init_ref().as_leaf())
                });
            }
            _ => {
                dbg_struct.field("root", unsafe {
                    &Some(DebugNode {
                        node: self.root.assume_init_ref().as_node(),
                        depth: self.depth,
                    })
                });
            }
        }
        dbg_struct.finish()
    }
}

impl<K, V> Drop for BTree<K, V> {
    fn drop(&mut self) {
        todo!()
    }
}
