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

use core::codec::*;
use core::index::{
    BinaryDocValues, FieldInfo, FieldInfos, MergeState, NumericDocValues, PointValues,
    SegmentCommitInfo, SegmentInfo, SegmentReadState, SegmentWriteState, SortedDocValues,
    SortedNumericDocValues, SortedSetDocValues,
};
use core::store::{Directory, IOContext, IndexOutput};
use core::util::bit_set::FixedBitSet;
use core::util::string_util::ID_LENGTH;
use core::util::{numeric::Numeric, Bits, BitsMut, BitsRef, BytesRef, ReusableIterator};

use error::{ErrorKind::IllegalArgument, Result};

use std::collections::HashSet;
use std::sync::Arc;

/// Encodes/decodes compound files
pub trait CompoundFormat {
    // TODO, this method should use a generic associated type as return type with is not
    // currently supported. so we just hard code the type because when we need return another
    // type the GAT must already supported

    /// Returns a Directory view (read-only) for the compound files in this segment
    fn get_compound_reader<D: Directory, C: Codec>(
        &self,
        dir: Arc<D>,
        si: &SegmentInfo<D, C>,
        ioctx: &IOContext,
    ) -> Result<Lucene50CompoundReader<D>>;

    /// Packs the provided segment's files into a compound format.  All files referenced
    /// by the provided {@link SegmentInfo} must have {@link CodecUtil#writeIndexHeader}
    /// and {@link CodecUtil#writeFooter}.
    fn write<D: Directory, DW: Directory, C: Codec>(
        &self,
        dir: &DW,
        si: &SegmentInfo<D, C>,
        ioctx: &IOContext,
    ) -> Result<()>;
}

/// Encodes/decodes terms, postings, and proximity data.
/// <p>
/// Note, when extending this class, the name ({@link #getName}) may
/// written into the index in certain configurations. In order for the segment
/// to be read, the name must resolve to your implementation via {@link #forName(String)}.
/// This method uses Java's
/// {@link ServiceLoader Service Provider Interface} (SPI) to resolve format names.
/// <p>
/// If you implement your own format, make sure that it has a no-arg constructor
/// so SPI can load it.
/// @see ServiceLoader
/// @lucene.experimental
pub trait PostingsFormat {
    type FieldsProducer: FieldsProducer;
    /// Reads a segment.  NOTE: by the time this call
    /// returns, it must hold open any files it will need to
    /// use; else, those files may be deleted.
    /// Additionally, required files may be deleted during the execution of
    /// this call before there is a chance to open them. Under these
    /// circumstances an IOException should be thrown by the implementation.
    /// IOExceptions are expected and will automatically cause a retry of the
    /// segment opening logic with the newly revised segments.
    fn fields_producer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::FieldsProducer>;

    /// Writes a new segment
    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<FieldsConsumerEnum<D, DW, C>>;

    /// Returns this posting format's name
    fn name(&self) -> &str;
}

/// composite `PostingsFormat` use for `CodecEnum`
pub enum PostingsFormatEnum {
    Lucene50(Lucene50PostingsFormat),
}

impl PostingsFormat for PostingsFormatEnum {
    type FieldsProducer = FieldsProducerEnum;

    fn fields_producer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::FieldsProducer> {
        match self {
            PostingsFormatEnum::Lucene50(f) => {
                Ok(FieldsProducerEnum::Lucene50(f.fields_producer(state)?))
            }
        }
    }

    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<FieldsConsumerEnum<D, DW, C>> {
        match self {
            PostingsFormatEnum::Lucene50(f) => f.fields_consumer(state),
        }
    }

    fn name(&self) -> &str {
        match self {
            PostingsFormatEnum::Lucene50(f) => f.name(),
        }
    }
}

pub fn postings_format_for_name(name: &str) -> Result<PostingsFormatEnum> {
    match name {
        "Lucene50" => Ok(PostingsFormatEnum::Lucene50(
            Lucene50PostingsFormat::default(),
        )),
        _ => bail!(IllegalArgument(format!(
            "Invalid postings format: {}",
            name
        ))),
    }
}

/// Controls the format of stored fields
pub trait StoredFieldsFormat {
    type Reader: StoredFieldsReader;
    /// Returns a {@link StoredFieldsReader} to load stored
    /// fields. */
    fn fields_reader<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        si: &SegmentInfo<D, C>,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Self::Reader>;

    /// Returns a {@link StoredFieldsWriter} to write stored
    /// fields
    fn fields_writer<D, DW, C>(
        &self,
        directory: Arc<DW>,
        si: &mut SegmentInfo<D, C>,
        ioctx: &IOContext,
    ) -> Result<StoredFieldsWriterEnum<DW::IndexOutput>>
    where
        D: Directory,
        DW: Directory,
        DW::IndexOutput: 'static,
        C: Codec;
}

/// Controls the format of term vectors
pub trait TermVectorsFormat {
    type TVReader: TermVectorsReader;
    /// Returns a {@link TermVectorsReader} to read term
    /// vectors.
    fn tv_reader<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        si: &SegmentInfo<D, C>,
        field_info: Arc<FieldInfos>,
        ioctx: &IOContext,
    ) -> Result<Self::TVReader>;

    /// Returns a {@link TermVectorsWriter} to write term
    /// vectors.
    fn tv_writer<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        segment_info: &SegmentInfo<D, C>,
        context: &IOContext,
    ) -> Result<TermVectorsWriterEnum<DW::IndexOutput>>;
}

// pub fn term_vectors_format_for_name(name: &str) -> Result<Box<TermVectorsFormat>> {
//    match name {
//        "Lucene50" | "Lucene53" | "Lucene54" | "Lucene60" | "Lucene62" => {
//            Ok(Box::new(lucene50::term_vectors_format()))
//        }
//        _ => bail!("Unknown format name"),
//    }
//}

/// Encodes/decodes `FieldInfos`
pub trait FieldInfosFormat {
    /// Read the `FieldInfos` previously written with {@link #write}. */
    fn read<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        segment_info: &SegmentInfo<D, C>,
        segment_suffix: &str,
        io_context: &IOContext,
    ) -> Result<FieldInfos>;

    /// Writes the provided `FieldInfos` to the directory.
    fn write<D: Directory, DW: Directory, C: Codec>(
        &self,
        directory: &DW,
        segment_info: &SegmentInfo<D, C>,
        segment_suffix: &str,
        infos: &FieldInfos,
        context: &IOContext,
    ) -> Result<()>;
}

/// Expert: Controls the format of the `SegmentInfo` (segment metadata file).
pub trait SegmentInfoFormat {
    /// Read `SegmentInfo` data from a directory.
    ///
    /// @param directory: directory to read from
    /// @param segment_name: name of the segment to read
    /// @param segment_id: expected identifier for the segment
    /// @return infos: instance to be populated with data
    fn read<D: Directory, C: Codec>(
        &self,
        directory: &Arc<D>,
        segment_name: &str,
        segment_id: [u8; ID_LENGTH],
        context: &IOContext,
    ) -> Result<SegmentInfo<D, C>>;

    /// Write `SegmentInfo` data.
    ///
    /// The codec must add its SegmentInfo filename(s) to {@code info} before doing i/o.
    /// @throws IOException If an I/O error occurs
    fn write<D: Directory, DW: Directory, C: Codec>(
        &self,
        dir: &Arc<DW>,
        info: &mut SegmentInfo<D, C>,
        io_context: &IOContext,
    ) -> Result<()>;
}

pub trait DocValuesFormat {
    fn name(&self) -> &str;
    // TODO need GAT to remove the Box
    fn fields_producer<'a, D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'a, D, DW, C>,
    ) -> Result<Box<dyn DocValuesProducer>>;

    // TODO need GAT
    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<DocValuesConsumerEnum<D, DW, C>>;
}

pub enum DocValuesFormatEnum {
    Lucene54(Lucene54DocValuesFormat),
    PerField(PerFieldDocValuesFormat),
}

impl DocValuesFormat for DocValuesFormatEnum {
    fn name(&self) -> &str {
        match self {
            DocValuesFormatEnum::Lucene54(d) => d.name(),
            DocValuesFormatEnum::PerField(d) => d.name(),
        }
    }

    fn fields_producer<'a, D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'a, D, DW, C>,
    ) -> Result<Box<dyn DocValuesProducer>> {
        match self {
            DocValuesFormatEnum::Lucene54(d) => d.fields_producer(state),
            DocValuesFormatEnum::PerField(d) => d.fields_producer(state),
        }
    }

    fn fields_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<DocValuesConsumerEnum<D, DW, C>> {
        match self {
            DocValuesFormatEnum::Lucene54(d) => d.fields_consumer(state),
            DocValuesFormatEnum::PerField(d) => d.fields_consumer(state),
        }
    }
}

pub enum DocValuesConsumerEnum<D: Directory, DW: Directory, C: Codec> {
    Lucene54(Lucene54DocValuesConsumer<DW::IndexOutput>),
    PerField(DocValuesFieldsWriter<D, DW, C>),
}

impl<D: Directory, DW: Directory, C: Codec> DocValuesConsumer for DocValuesConsumerEnum<D, DW, C> {
    fn add_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => d.add_numeric_field(field_info, values),
            DocValuesConsumerEnum::PerField(d) => d.add_numeric_field(field_info, values),
        }
    }

    fn add_binary_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => d.add_binary_field(field_info, values),
            DocValuesConsumerEnum::PerField(d) => d.add_binary_field(field_info, values),
        }
    }

    fn add_sorted_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => {
                d.add_sorted_field(field_info, values, doc_to_ord)
            }
            DocValuesConsumerEnum::PerField(d) => {
                d.add_sorted_field(field_info, values, doc_to_ord)
            }
        }
    }

    fn add_sorted_numeric_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
        doc_to_value_count: &mut impl ReusableIterator<Item = Result<u32>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => {
                d.add_sorted_numeric_field(field_info, values, doc_to_value_count)
            }
            DocValuesConsumerEnum::PerField(d) => {
                d.add_sorted_numeric_field(field_info, values, doc_to_value_count)
            }
        }
    }

    fn add_sorted_set_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<BytesRef>>,
        doc_to_ord_count: &mut impl ReusableIterator<Item = Result<u32>>,
        ords: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => {
                d.add_sorted_set_field(field_info, values, doc_to_ord_count, ords)
            }
            DocValuesConsumerEnum::PerField(d) => {
                d.add_sorted_set_field(field_info, values, doc_to_ord_count, ords)
            }
        }
    }

    fn merge<D1: Directory, C1: Codec>(
        &mut self,
        merge_state: &mut MergeState<D1, C1>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => d.merge(merge_state),
            DocValuesConsumerEnum::PerField(d) => d.merge(merge_state),
        }
    }

    fn merge_numeric_field<D1: Directory, C1: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D1, C1>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
        docs_with_field: Vec<Box<dyn BitsMut>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => {
                d.merge_numeric_field(field_info, merge_state, to_merge, docs_with_field)
            }
            DocValuesConsumerEnum::PerField(d) => {
                d.merge_numeric_field(field_info, merge_state, to_merge, docs_with_field)
            }
        }
    }

    fn merge_binary_field<D1: Directory, C1: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D1, C1>,
        to_merge: Vec<Box<dyn BinaryDocValues>>,
        docs_with_field: Vec<Box<dyn BitsMut>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => {
                d.merge_binary_field(field_info, merge_state, to_merge, docs_with_field)
            }
            DocValuesConsumerEnum::PerField(d) => {
                d.merge_binary_field(field_info, merge_state, to_merge, docs_with_field)
            }
        }
    }

    fn merge_sorted_field<D1: Directory, C1: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D1, C1>,
        to_merge: Vec<Box<dyn SortedDocValues>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => {
                d.merge_sorted_field(field_info, merge_state, to_merge)
            }
            DocValuesConsumerEnum::PerField(d) => {
                d.merge_sorted_field(field_info, merge_state, to_merge)
            }
        }
    }

    fn merge_sorted_set_field<D1: Directory, C1: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D1, C1>,
        to_merge: Vec<Box<dyn SortedSetDocValues>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => {
                d.merge_sorted_set_field(field_info, merge_state, to_merge)
            }
            DocValuesConsumerEnum::PerField(d) => {
                d.merge_sorted_set_field(field_info, merge_state, to_merge)
            }
        }
    }

    fn merge_sorted_numeric_field<D1: Directory, C1: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D1, C1>,
        to_merge: Vec<Box<dyn SortedNumericDocValues>>,
    ) -> Result<()> {
        match self {
            DocValuesConsumerEnum::Lucene54(d) => {
                d.merge_sorted_numeric_field(field_info, merge_state, to_merge)
            }
            DocValuesConsumerEnum::PerField(d) => {
                d.merge_sorted_numeric_field(field_info, merge_state, to_merge)
            }
        }
    }
}

pub fn doc_values_format_for_name(format: &str) -> Result<DocValuesFormatEnum> {
    match format {
        "Lucene54" => Ok(DocValuesFormatEnum::Lucene54(
            Lucene54DocValuesFormat::default(),
        )),
        _ => unimplemented!(),
    }
}

/// Encodes/decodes per-document score normalization values.
pub trait NormsFormat {
    type NormsProducer: NormsProducer;
    /// Returns a {@link NormsProducer} to read norms from the index.
    /// <p>
    /// NOTE: by the time this call returns, it must hold open any files it will
    /// need to use; else, those files may be deleted. Additionally, required files
    /// may be deleted during the execution of this call before there is a chance
    /// to open them. Under these circumstances an IOException should be thrown by
    /// the implementation. IOExceptions are expected and will automatically cause
    /// a retry of the segment opening logic with the newly revised segments.
    fn norms_producer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::NormsProducer>;

    fn norms_consumer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<NormsConsumerEnum<DW::IndexOutput>>;
}

pub enum NormsConsumerEnum<O: IndexOutput> {
    Lucene53(Lucene53NormsConsumer<O>),
}

impl<O: IndexOutput> NormsConsumer for NormsConsumerEnum<O> {
    fn add_norms_field(
        &mut self,
        field_info: &FieldInfo,
        values: &mut impl ReusableIterator<Item = Result<Numeric>>,
    ) -> Result<()> {
        match self {
            NormsConsumerEnum::Lucene53(w) => w.add_norms_field(field_info, values),
        }
    }

    fn merge_norms_field<D: Directory, C: Codec>(
        &mut self,
        field_info: &FieldInfo,
        merge_state: &MergeState<D, C>,
        to_merge: Vec<Box<dyn NumericDocValues>>,
    ) -> Result<()> {
        match self {
            NormsConsumerEnum::Lucene53(w) => {
                w.merge_norms_field(field_info, merge_state, to_merge)
            }
        }
    }

    fn merge<D: Directory, C: Codec>(&mut self, merge_state: &mut MergeState<D, C>) -> Result<()> {
        match self {
            NormsConsumerEnum::Lucene53(w) => w.merge(merge_state),
        }
    }
}

/// Format for live/deleted documents
pub trait LiveDocsFormat {
    /// Creates a new Bits, with all bits set, for the specified size.
    fn new_live_docs(&self, size: usize) -> Result<FixedBitSet>;
    /// Creates a new mutablebits of the same bits set and size of existing.
    fn new_live_docs_from_existing(&self, existing: &dyn Bits) -> Result<BitsRef>;
    /// Read live docs bits.
    fn read_live_docs<D: Directory, C: Codec>(
        &self,
        dir: Arc<D>,
        info: &SegmentCommitInfo<D, C>,
        context: &IOContext,
    ) -> Result<BitsRef>;

    /// Persist live docs bits.  Use {@link
    /// SegmentCommitInfo#getNextDelGen} to determine the
    /// generation of the deletes file you should write to. */
    fn write_live_docs<D: Directory, D1: Directory, C: Codec>(
        &self,
        bits: &dyn Bits,
        dir: &D1,
        info: &SegmentCommitInfo<D, C>,
        new_del_count: i32,
        context: &IOContext,
    ) -> Result<()>;

    /// Records all files in use by this {@link SegmentCommitInfo} into the files argument. */
    fn files<D: Directory, C: Codec>(
        &self,
        info: &SegmentCommitInfo<D, C>,
        files: &mut HashSet<String>,
    );
}

/// Encodes/decodes indexed points.
/// @lucene.experimental */
pub trait PointsFormat {
    //    type Writer<D, DW>: PointsWriter<D, DW>;
    type Reader: PointValues;

    /// Writes a new segment
    //    fn fields_writer<D: Directory, DW: Directory>(
    //        &self,
    //        state: &SegmentWriteState<D, DW, C>
    //    ) -> Result<Self::Writer<D, DW>>;
    // TODO we need GAT to make this interface possible
    fn fields_writer<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentWriteState<D, DW, C>,
    ) -> Result<Lucene60PointsWriter<D, DW, C>>;

    /// Reads a segment.  NOTE: by the time this call
    /// returns, it must hold open any files it will need to
    /// use; else, those files may be deleted.
    /// Additionally, required files may be deleted during the execution of
    /// this call before there is a chance to open them. Under these
    /// circumstances an IOException should be thrown by the implementation.
    /// IOExceptions are expected and will automatically cause a retry of the
    /// segment opening logic with the newly revised segments.
    fn fields_reader<D: Directory, DW: Directory, C: Codec>(
        &self,
        state: &SegmentReadState<'_, D, DW, C>,
    ) -> Result<Self::Reader>;
}
