// Copyright 2019 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use core::util::fst::{Arc, FSTBytesReader, OutputFactory, END_LABEL, FST};
use error::Result;

pub struct FSTIterBase<F: OutputFactory> {
    fst: FST<F>,
    arcs: Vec<Arc<F::Value>>,
    output: Vec<F::Value>,
    fst_reader: FSTBytesReader, // a reader of self.fst
    upto: usize,
    target_length: usize,
}

impl<F: OutputFactory> FSTIterBase<F> {
    fn new(fst: FST<F>) -> Self {
        let fst_reader = fst.bytes_reader();
        let no_output = fst.outputs().empty();
        let arc = fst.root_arc();
        let mut arcs = Vec::with_capacity(10);
        arcs.push(arc);
        for _ in 1..arcs.capacity() {
            arcs.push(Arc::empty());
        }
        let mut output = Vec::with_capacity(10);
        for _i in 0..10 {
            output.push(no_output.clone());
        }

        FSTIterBase {
            fst,
            arcs,
            output,
            fst_reader,
            upto: 0,
            target_length: 0,
        }
    }

    // Call this after self.fst is moved, then self.fst_reader is invalid
    fn init(&mut self) {
        let reader = self.fst.bytes_reader();
        self.fst_reader = reader;
    }

    fn read_first_target_arc(&mut self, arc_idx: usize, into_idx: usize) -> Result<()> {
        let arc = self
            .fst
            .read_first_target_arc(&self.arcs[arc_idx], &mut self.fst_reader)?;
        self.arcs[into_idx] = arc;
        Ok(())
    }

    fn read_next_arc(&mut self, arc_idx: usize) -> Result<()> {
        self.fst
            .read_next_arc(&mut self.arcs[arc_idx], &mut self.fst_reader)
    }

    fn add_output(&self, output_idx: usize, arc_idx: usize) -> F::Value {
        if let Some(ref output) = self.arcs[arc_idx].output {
            self.fst.outputs().add(&self.output[output_idx], output)
        } else {
            self.output[output_idx].clone()
        }
    }

    fn grow(&mut self) {
        if self.arcs.len() <= self.upto {
            for _ in self.arcs.len()..=self.upto {
                self.arcs.push(Arc::empty());
            }
        }
        if self.output.len() <= self.upto {
            for _ in self.output.len()..=self.upto {
                let o = self.fst.outputs().empty();
                self.output.push(o);
            }
        }
    }
}

pub trait FSTIterator<F: OutputFactory> {
    fn get_target_label(&self) -> i32;
    fn get_current_label(&self) -> i32;
    fn set_current_label(&mut self, label: i32);
    fn grow(&mut self);
    fn iter_base(&self) -> &FSTIterBase<F>;
    fn iter_base_mut(&mut self) -> &mut FSTIterBase<F>;

    // Rewinds enum state to match the shared prefix between current term and target term
    fn rewind_prefix(&mut self) -> Result<()> {
        if self.iter_base().upto == 0 {
            self.iter_base_mut().upto = 1;
            self.iter_base_mut().read_first_target_arc(0, 1)?;
            return Ok(());
        }

        let current_limit = self.iter_base().upto;
        let mut upto = 1;
        loop {
            if upto >= current_limit || upto > self.iter_base().target_length + 1 {
                break;
            }
            let cmp = self.get_current_label() - self.get_target_label();
            if cmp < 0 {
                // seek forward
                break;
            } else if cmp > 0 {
                self.iter_base_mut().read_first_target_arc(upto - 1, upto)?;
                break;
            }
            upto += 1;
        }
        self.iter_base_mut().upto = upto;
        Ok(())
    }

    fn do_next(&mut self) -> Result<()> {
        let mut upto = self.iter_base().upto;
        if upto == 0 {
            upto = 1;
            self.iter_base_mut().read_first_target_arc(0, 1)?;
        } else {
            loop {
                if !self.iter_base().arcs[upto].is_last() {
                    break;
                }
                upto -= 1;
                if upto == 0 {
                    self.iter_base_mut().upto = upto;
                    return Ok(());
                }
            }
            self.iter_base_mut().read_next_arc(upto)?;
        }
        self.iter_base_mut().upto = upto;
        self.push_first()
    }

    // TODO: should we return a status here (SEEK_FOUND / SEEK_NOT_FOUND /
    // SEEK_END)?  saves the eq check above?

    /// Seeks to smallest term that's >= target.
    //    pub fn do_seek_ceil() -> Result<()> {
    //        unimplemented!()
    //    }

    // Appends current arc, and then recurses from its target,
    // appending first arc all the way to the final node
    fn push_first(&mut self) -> Result<()> {
        loop {
            let upto = self.iter_base().upto;
            let output = self.iter_base().add_output(upto - 1, upto);
            self.iter_base_mut().output[upto] = output;
            let label = self.iter_base().arcs[upto].label;
            if label == END_LABEL {
                // Final node
                break;
            }
            self.set_current_label(label);
            self.incr();
            let new_upto = self.iter_base().upto;
            self.iter_base_mut().read_first_target_arc(upto, new_upto)?;
        }
        Ok(())
    }

    fn incr(&mut self) {
        self.iter_base_mut().upto += 1;
        self.grow();
        self.iter_base_mut().grow();
    }
}

/// Enumerates all input (BytesRef) + output pairs in an FST.
pub struct BytesRefFSTIterator<F: OutputFactory> {
    base: FSTIterBase<F>,
    current: Vec<u8>,
    // target: Vec<u8>,
}

impl<F: OutputFactory> BytesRefFSTIterator<F> {
    pub fn new(fst: FST<F>) -> Self {
        let base = FSTIterBase::new(fst);
        let current = vec![0u8; 10];
        BytesRefFSTIterator { base, current }
    }

    pub fn init(&mut self) {
        self.base.init();
    }

    pub fn next(&mut self) -> Result<Option<(&[u8], F::Value)>> {
        self.do_next()?;
        if self.base.upto == 0 {
            Ok(None)
        } else {
            let upto = self.base.upto;
            let output = self.base.output[upto].clone();
            let input = &self.current[1..upto];
            Ok(Some((input, output)))
        }
    }
}

impl<F: OutputFactory> FSTIterator<F> for BytesRefFSTIterator<F> {
    fn get_target_label(&self) -> i32 {
        unimplemented!()
    }

    fn get_current_label(&self) -> i32 {
        self.current[self.base.upto] as u32 as i32
    }

    fn set_current_label(&mut self, label: i32) {
        let idx = self.base.upto;
        self.current[idx] = label as u8;
    }

    fn grow(&mut self) {
        let new_size = self.base.upto + 1;
        self.current.resize(new_size, 0u8);
    }

    fn iter_base(&self) -> &FSTIterBase<F> {
        &self.base
    }

    fn iter_base_mut(&mut self) -> &mut FSTIterBase<F> {
        &mut self.base
    }
}
