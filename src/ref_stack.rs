use core::marker::PhantomData;

pub struct RefStack<'a, T, const N: usize> {
    stack: [*mut T; N],
    len: usize,
    phantom: PhantomData<&'a mut T>,
}

impl<'a, T, const N: usize> RefStack<'a, T, N> {
    #[inline]
    pub fn new() -> Self {
        Self {
            stack: unsafe { core::mem::MaybeUninit::uninit().assume_init() },
            len: 0,
            phantom: PhantomData,
        }
    }

    #[inline]
    pub fn with_root(root: &'a mut T) -> Self {
        let mut slf = Self::new();
        slf.set_root(root);

        slf
    }

    #[inline]
    pub fn set_root(&mut self, root: &'a mut T) -> Option<&'a mut T> {
        let prev_root = if self.is_empty() {
            None
        } else {
            unsafe { Some(&mut *self.stack[0]) }
        };
        self.len = 1;
        self.stack[0] = root as _;
        prev_root
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len == N
    }

    #[inline]
    pub fn push<F: FnOnce(&mut T) -> &mut T>(&mut self, f: F) {
        assert!(self.ret_push(f).is_none());
    }

    #[inline]
    pub fn ret_push<F: FnOnce(&mut T) -> &mut T>(&mut self, f: F) -> Option<F> {
        if self.is_empty() || self.is_full() {
            Some(f)
        } else {
            unsafe {
                self.stack[self.len] = f(&mut *self.stack[self.len() - 1]) as _;
                self.len += 1;
            }
            None
        }
    }

    #[inline]
    pub fn pop(&mut self) -> Option<&'a mut T> {
        self.len -= 1;
        if self.is_empty() {
            unsafe { Some(&mut *self.stack[0]) }
        } else {
            None
        }
    }

    #[inline]
    pub fn top(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            unsafe { Some(&*self.stack[self.len - 1]) }
        }
    }

    #[inline]
    pub fn top_mut(&mut self) -> Option<&mut T> {
        if self.is_empty() {
            None
        } else {
            unsafe { Some(&mut *self.stack[self.len - 1]) }
        }
    }
}
