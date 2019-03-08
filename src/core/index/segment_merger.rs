use core::codec::Codec;
use core::index::merge_state::MergeState;
use core::index::{FieldInfosBuilder, FieldNumbersRef};
use core::index::{SegmentInfo, SegmentReader, SegmentWriteState};
use core::store::{DirectoryRc, IOContext};
use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;

use std::mem;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// The SegmentMerger class combines two or more Segments, represented by an
/// IndexReader, into a single Segment.  Call the merge method to combine the
/// segments
pub struct SegmentMerger {
    directory: DirectoryRc,
    codec: Arc<Codec>,
    context: IOContext,
    pub merge_state: MergeState,
    field_infos_builder: FieldInfosBuilder<FieldNumbersRef>,
}

impl SegmentMerger {
    pub fn new(
        readers: Vec<Arc<SegmentReader>>,
        segment_info: &SegmentInfo,
        directory: DirectoryRc,
        field_numbers: FieldNumbersRef,
        context: IOContext,
    ) -> Result<Self> {
        if !context.is_merge() {
            bail!(IllegalArgument("IOContext should be merge!".into()));
        }
        let codec = segment_info.codec().clone();
        let merge_state = MergeState::new(readers, segment_info)?;
        let field_infos_builder = FieldInfosBuilder::new(field_numbers);
        Ok(SegmentMerger {
            directory,
            codec,
            context,
            merge_state,
            field_infos_builder,
        })
    }

    /// True if any merging should happen
    pub fn should_merge(&self) -> bool {
        self.merge_state.segment_info().max_doc() > 0
    }

    /// Merges the readers into the directory passed to the constructor
    pub fn merge(&mut self) -> Result<()> {
        if !self.should_merge() {
            bail!(IllegalState(
                "Merge would result in 0 ducument segment".into()
            ));
        }
        self.merge_field_infos()?;

        let start = SystemTime::now();
        let num_merged = self.merge_fields()?;
        let after_fields = SystemTime::now();
        //        println!(
        //            "merge {} fields costs {:?}",
        //            self.merge_state.segment_info().name,
        //            after_fields.duration_since(start).unwrap()
        //        );
        assert_eq!(num_merged, self.merge_state.segment_info().max_doc);

        let segment_write_state = SegmentWriteState::new(
            Arc::clone(&self.directory),
            self.merge_state.segment_info().clone(),
            self.merge_state
                .merge_field_infos
                .as_ref()
                .unwrap()
                .as_ref()
                .clone(),
            None,
            self.context.clone(),
            "".into(),
        );
        self.merge_terms(&segment_write_state)?;
        let after_terms = SystemTime::now();
        //        println!(
        //            "merge {} {} terms costs {:?}",
        //            self.merge_state.segment_info().name,
        //            self.merge_state.segment_info().max_doc(),
        //            after_terms.duration_since(after_fields).unwrap()
        //        );

        if self
            .merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .has_doc_values
        {
            self.merge_doc_values(&segment_write_state)?;
        }
        let after_dvs = SystemTime::now();
        //        println!(
        //            "merge {} {} doc values costs {:?}",
        //            self.merge_state.segment_info().name,
        //            self.merge_state.segment_info().max_doc(),
        //            after_dvs.duration_since(after_terms).unwrap()
        //        );
        if self
            .merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .has_point_values
        {
            self.merge_points(&segment_write_state)?;
        }
        let after_points = SystemTime::now();

        if self
            .merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .has_norms
        {
            self.merge_norms(&segment_write_state)?;
        }
        if self
            .merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .has_vectors
        {
            let num_merged = self.merge_vectors()?;
            assert_eq!(num_merged, self.merge_state.segment_info().max_doc);
        }
        let after_tvs = SystemTime::now();
        //        println!(
        //            "merge {} {} term vector costs {:?}",
        //            self.merge_state.segment_info().name,
        //            self.merge_state.segment_info().max_doc(),
        //            after_tvs.duration_since(after_points).unwrap()
        //        );
        self.codec.field_infos_format().write(
            self.directory.as_ref(),
            self.merge_state.segment_info(),
            "",
            self.merge_state
                .merge_field_infos
                .as_ref()
                .unwrap()
                .as_ref(),
            &self.context,
        )?;
        println!(
            "merge {} {} total cost {:?}",
            self.merge_state.segment_info().name,
            self.merge_state.segment_info().max_doc(),
            SystemTime::now().duration_since(start).unwrap()
        );
        Ok(())
    }

    fn merge_doc_values(&mut self, segment_write_state: &SegmentWriteState) -> Result<()> {
        let mut consumer = self
            .codec
            .doc_values_format()
            .fields_consumer(segment_write_state)?;
        consumer.merge(&mut self.merge_state)
    }

    fn merge_points(&mut self, segment_write_state: &SegmentWriteState) -> Result<()> {
        let mut writer = self
            .codec
            .points_format()
            .fields_writer(segment_write_state)?;
        writer.merge(&mut self.merge_state)
    }

    fn merge_norms(&mut self, segment_write_state: &SegmentWriteState) -> Result<()> {
        let mut consumer = self
            .codec
            .norms_format()
            .norms_consumer(segment_write_state)?;
        consumer.merge(&mut self.merge_state)
    }

    pub fn merge_field_infos(&mut self) -> Result<()> {
        let temp_builder =
            FieldInfosBuilder::new(self.field_infos_builder.global_field_numbers.clone());
        let mut builder = mem::replace(&mut self.field_infos_builder, temp_builder);
        for reader_field_infos in &self.merge_state.fields_infos {
            for fi in reader_field_infos.by_number.values() {
                builder.add(fi.as_ref())?;
            }
        }
        self.merge_state.merge_field_infos = Some(Arc::new(builder.finish()?));
        // debug_assert!(
        //     !self.merge_state
        //         .merge_field_infos
        //         .as_ref()
        //         .unwrap()
        //         .has_norms
        // );
        self.field_infos_builder = builder;
        Ok(())
    }

    /// Merge stored fields from each of the segments into the new one.
    fn merge_fields(&mut self) -> Result<i32> {
        let mut fields_writer = self.codec.stored_fields_format().fields_writer(
            Arc::clone(&self.directory),
            self.merge_state.segment_info(),
            &self.context,
        )?;
        fields_writer.merge(&mut self.merge_state)
    }

    /// Merge the TermVectors from each of the segments into the new one.
    fn merge_vectors(&mut self) -> Result<i32> {
        let mut term_vectors_writer = self.codec.term_vectors_format().tv_writer(
            Arc::clone(&self.directory),
            self.merge_state.segment_info(),
            &self.context,
        )?;
        term_vectors_writer.merge(&mut self.merge_state)
    }

    fn merge_terms(&mut self, segment_write_state: &SegmentWriteState) -> Result<()> {
        let mut consumer = self
            .codec
            .postings_format()
            .fields_consumer(segment_write_state)?;
        consumer.merge(&mut self.merge_state)
    }
}
