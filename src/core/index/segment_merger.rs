use core::codec::{
    Codec, DocValuesConsumer, DocValuesFormat, FieldInfosFormat, FieldsConsumer, NormsConsumer,
    NormsFormat, PointsFormat, PointsWriter, PostingsFormat, StoredFieldsFormat,
    StoredFieldsWriter, TermVectorsFormat, TermVectorsWriter,
};
use core::index::merge_state::MergeState;
use core::index::{FieldInfosBuilder, FieldNumbersRef};
use core::index::{SegmentInfo, SegmentReader, SegmentWriteState};
use core::store::{Directory, IOContext};
use error::ErrorKind::{IllegalArgument, IllegalState};
use error::Result;

use std::mem;
use std::sync::Arc;

/// The SegmentMerger class combines two or more Segments, represented by an
/// IndexReader, into a single Segment.  Call the merge method to combine the
/// segments
pub struct SegmentMerger<D: Directory + 'static, DW: Directory, C: Codec> {
    directory: Arc<DW>,
    codec: Arc<C>,
    context: IOContext,
    pub merge_state: MergeState<D, C>,
    field_infos_builder: FieldInfosBuilder<FieldNumbersRef>,
}

impl<D, DW, C> SegmentMerger<D, DW, C>
where
    D: Directory + 'static,
    DW: Directory + 'static,
    C: Codec,
{
    pub fn new(
        readers: Vec<Arc<SegmentReader<D, C>>>,
        segment_info: &SegmentInfo<D, C>,
        directory: Arc<DW>,
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

        let num_merged = self.merge_fields()?;
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

        if self
            .merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .has_doc_values
        {
            self.merge_doc_values(&segment_write_state)?;
        }
        if self
            .merge_state
            .merge_field_infos
            .as_ref()
            .unwrap()
            .has_point_values
        {
            self.merge_points(&segment_write_state)?;
        }

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

        Ok(())
    }

    fn merge_doc_values(
        &mut self,
        segment_write_state: &SegmentWriteState<D, DW, C>,
    ) -> Result<()> {
        let mut consumer = self
            .codec
            .doc_values_format()
            .fields_consumer(segment_write_state)?;
        consumer.merge(&mut self.merge_state)
    }

    fn merge_points(&mut self, segment_write_state: &SegmentWriteState<D, DW, C>) -> Result<()> {
        let mut writer = self
            .codec
            .points_format()
            .fields_writer(segment_write_state)?;
        writer.merge(&mut self.merge_state)
    }

    fn merge_norms(&mut self, segment_write_state: &SegmentWriteState<D, DW, C>) -> Result<()> {
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
            &*self.directory,
            self.merge_state.segment_info(),
            &self.context,
        )?;
        term_vectors_writer.merge(&mut self.merge_state)
    }

    fn merge_terms(&mut self, segment_write_state: &SegmentWriteState<D, DW, C>) -> Result<()> {
        let mut consumer = self
            .codec
            .postings_format()
            .fields_consumer(segment_write_state)?;
        consumer.merge(&mut self.merge_state)
    }
}
