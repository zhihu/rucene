use core::search::Scorer;
use core::util::DocId;
use error::Result;

use std::cmp::{Ord, Ordering};
use std::ptr;

#[derive(Eq)]
pub struct DisiWrapper {
    scorer: Box<Scorer>,
    pub doc: DocId,
    matches: Option<bool>,
    next: *mut DisiWrapper,
}

impl DisiWrapper {
    pub fn next_scorer(&self) -> Option<&mut DisiWrapper> {
        if self.next.is_null() {
            None
        } else {
            unsafe { Some(&mut *self.next) }
        }
    }

    pub fn new(scorer: Box<Scorer>) -> DisiWrapper {
        DisiWrapper {
            scorer,
            doc: -1,
            matches: None,
            next: ptr::null_mut(),
        }
    }

    pub fn scorer(&mut self) -> &mut Scorer {
        self.scorer.as_mut()
    }

    pub fn set_doc(&mut self, doc: DocId) {
        if self.doc != doc {
            self.matches = None;
        }
        self.doc = doc;
    }

    pub fn doc(&self) -> DocId {
        self.scorer.doc_id()
    }

    pub fn score(&mut self) -> Result<f32> {
        self.scorer().score()
    }

    pub fn support_two_phase(&self) -> bool {
        self.scorer.support_two_phase()
    }

    pub fn doc_id(&self) -> DocId {
        self.scorer.doc_id()
    }

    pub fn next(&mut self) -> Result<DocId> {
        let doc_id = self.scorer().next()?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    pub fn advance(&mut self, target: DocId) -> Result<DocId> {
        let doc_id = self.scorer().advance(target)?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    pub fn matches(&mut self) -> Result<bool> {
        if self.matches.is_none() {
            self.matches = Some(self.scorer().matches()?);
        }

        Ok(self.matches.unwrap())
    }

    pub fn match_cost(&self) -> f32 {
        self.scorer.match_cost()
    }

    pub fn approximate_next(&mut self) -> Result<DocId> {
        let doc_id = self.scorer().approximate_next()?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    pub fn approximate_advance(&mut self, target: DocId) -> Result<DocId> {
        let doc_id = self.scorer().approximate_advance(target)?;
        self.set_doc(doc_id);
        Ok(doc_id)
    }

    pub fn cost(&self) -> usize {
        self.scorer.cost()
    }
}

impl Ord for DisiWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.scorer.doc_id().cmp(&other.scorer.doc_id()).reverse()
    }
}

impl PartialEq for DisiWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.scorer.doc_id() == other.scorer.doc_id()
    }
}

impl PartialOrd for DisiWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct DisiPriorityQueue {
    heap: Vec<*mut DisiWrapper>,
    size: isize,
    _buffer: Vec<DisiWrapper>,
}

impl DisiPriorityQueue {
    fn left_node(node: isize) -> isize {
        ((node + 1) << 1) - 1
    }

    fn right_node(left_node: isize) -> isize {
        left_node + 1
    }

    fn parent_node(node: isize) -> isize {
        ((node + 1) >> 1) - 1
    }

    pub fn new(children: Vec<Box<Scorer>>) -> DisiPriorityQueue {
        let mut _buffer: Vec<DisiWrapper> = children.into_iter().map(DisiWrapper::new).collect();
        let children: Vec<*mut DisiWrapper> =
            _buffer.iter_mut().map(|x| x as *mut DisiWrapper).collect();
        let mut queue = DisiPriorityQueue {
            heap: vec![ptr::null_mut(); children.len()],
            size: 0,
            _buffer,
        };

        for disi in children {
            unsafe {
                queue.do_push(disi);
            }
        }
        queue
    }

    pub fn size(&self) -> usize {
        self.size as usize
    }

    pub fn top(&self) -> &'static mut DisiWrapper {
        unsafe { &mut *self.do_top() }
    }

    fn do_top(&self) -> *mut DisiWrapper {
        self.heap[0]
    }

    /// Get the list of scorers which are on the current doc.
    pub fn top_list(&self) -> &mut DisiWrapper {
        unsafe { &mut *self.do_top_list() }
    }

    unsafe fn do_top_list(&self) -> *mut DisiWrapper {
        let heap = &self.heap;
        let size = self.size;
        let mut list = heap[0];
        (*list).next = ptr::null_mut();
        if size >= 3 {
            list = Self::top_list_to(list, heap, size, 1);
            list = Self::top_list_to(list, heap, size, 2);
        } else if size == 2 && (*heap[1]).doc() == (*list).doc() {
            list = Self::prepend(heap[1], list);
        }
        list
    }

    unsafe fn prepend(w1: *mut DisiWrapper, w2: *mut DisiWrapper) -> *mut DisiWrapper {
        (*w1).next = w2;
        w1
    }

    unsafe fn top_list_to(
        mut list: *mut DisiWrapper,
        heap: &Vec<*mut DisiWrapper>,
        size: isize,
        i: isize,
    ) -> *mut DisiWrapper {
        let w = heap[i as usize];
        if (*w).doc() == (*list).doc() {
            list = Self::prepend(w, list);
            let left = Self::left_node(i);
            let right = left + 1;
            if right < size {
                list = Self::top_list_to(list, heap, size, left);
                list = Self::top_list_to(list, heap, size, right);
            } else if left < size && (*heap[left as usize]).doc() == (*list).doc() {
                list = Self::prepend(heap[left as usize], list);
            }
        }
        list
    }

    pub fn push(&mut self, entry: &mut DisiWrapper) {
        unsafe {
            self.do_push(entry as *mut DisiWrapper);
        }
    }

    unsafe fn do_push(&mut self, entry: *mut DisiWrapper) -> *mut DisiWrapper {
        self.heap[self.size as usize] = entry;
        let size = self.size;
        self.up_heap(size);
        self.size += 1;
        self.heap[0]
    }

    pub fn pop(&mut self) -> &'static mut DisiWrapper {
        unsafe { &mut *self.do_pop() }
    }

    unsafe fn do_pop(&mut self) -> *mut DisiWrapper {
        let result = self.heap[0];
        self.size -= 1;
        let size = self.size;
        self.heap[0] = self.heap[size as usize];
        self.heap[size as usize] = ptr::null_mut();
        self.down_heap(size);
        result
    }

    pub fn peek(&self) -> &mut DisiWrapper {
        unsafe {
            let result = self.heap[0];
            &mut *result
        }
    }

    pub fn update_top(&mut self) -> &'static mut DisiWrapper {
        unsafe { &mut *self.do_update_top() }
    }

    unsafe fn do_update_top(&mut self) -> *mut DisiWrapper {
        let size = self.size;
        self.down_heap(size);
        self.heap[0]
    }

    pub fn update_top_with(
        &mut self,
        top_replacement: &mut DisiWrapper,
    ) -> &'static mut DisiWrapper {
        unsafe { &mut *self.do_update_top_with(top_replacement as *mut DisiWrapper) }
    }

    unsafe fn do_update_top_with(&mut self, top_replacement: *mut DisiWrapper) -> *mut DisiWrapper {
        self.heap[0] = top_replacement;
        self.update_top()
    }

    unsafe fn up_heap(&mut self, mut i: isize) {
        let node = self.heap[i as usize];
        let node_doc = (*node).doc();
        let mut j = Self::parent_node(i);
        while j >= 0 && node_doc < (*self.heap[j as usize]).doc() {
            self.heap[i as usize] = self.heap[j as usize];
            i = j;
            j = Self::parent_node(j);
        }
        self.heap[i as usize] = node;
    }

    unsafe fn down_heap(&mut self, size: isize) {
        let mut i = 0;
        let node = self.heap[0];
        let mut j = Self::left_node(i);
        if j < self.size {
            let mut k = Self::right_node(j);
            if k < size && (*self.heap[k as usize]).doc() < (*self.heap[j as usize]).doc() {
                j = k;
            }
            if (*self.heap[j as usize]).doc() < (*node).doc() {
                loop {
                    self.heap[i as usize] = self.heap[j as usize];
                    i = j;
                    j = Self::left_node(i);
                    k = Self::right_node(j);
                    if k < size && (*self.heap[k as usize]).doc() < (*self.heap[j as usize]).doc() {
                        j = k;
                    }
                    if j >= size || (*self.heap[j as usize]).doc() >= (*node).doc() {
                        break;
                    }
                }
                self.heap[i as usize] = node;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}
