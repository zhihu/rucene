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

use error::{ErrorKind, Result};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use core::codec::{codec_util, Codec};
use core::store::directory::Directory;
use core::store::io::{DataInput, DataOutput, IndexInput, IndexOutput};
use core::store::IOContext;

use core::codec::segment_infos::{segment_file_name, strip_segment_name, SegmentInfo};

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

const DATA_EXTENSION: &str = "cfs";
/// Extension of compound file entries */
pub const ENTRIES_EXTENSION: &str = "cfe";
pub const DATA_CODEC: &str = "Lucene50CompoundData";
pub const ENTRY_CODEC: &str = "Lucene50CompoundEntries";
pub const VERSION_START: i32 = 0;
pub const VERSION_CURRENT: i32 = VERSION_START;

/// Lucene 5.0 compound file format
///
/// Files:
///  * .cfs: An optional "virtual" file consisting of all the other index files for systems that
///    frequently run out of file handles.
///  * .cfe: The "virtual" compound file's entry table holding all entries in the corresponding .cfs
///    file.
///
/// Description:
///  * Compound (.cfs) --> Header, FileData <sup>FileCount</sup>, Footer
///  * Compound Entry Table (.cfe) --> Header, FileCount, <FileName, DataOffset, DataLength>
///    <sup>FileCount</sup>
///  * Header --> {@link codec_util#write_index_header IndexHeader}
///  * FileCount --> {@link DataOutput#writeVInt VInt}
///  * DataOffset,DataLength,Checksum --> {@link DataOutput#writeLong UInt64}
///  * FileName --> {@link DataOutput#writeString String}
///  * FileData --> raw file data
/// Footer --> {@link CodecUtil#writeFooter CodecFooter}
///
/// Notes:
///  * FileCount indicates how many files are contained in this compound file. The entry table that
///    follows has that many entries.
///  * Each directory entry contains a long pointer to the start of this file's data section, the
///    files length, and a String with that file's name.
#[derive(Copy, Clone)]
pub struct Lucene50CompoundFormat;

impl CompoundFormat for Lucene50CompoundFormat {
    fn get_compound_reader<D: Directory, C: Codec>(
        &self,
        dir: Arc<D>,
        si: &SegmentInfo<D, C>,
        io_ctx: &IOContext,
    ) -> Result<Lucene50CompoundReader<D>> {
        Lucene50CompoundReader::new(dir, si, io_ctx)
    }

    fn write<D: Directory, DW: Directory, C: Codec>(
        &self,
        dir: &DW,
        si: &SegmentInfo<D, C>,
        io_ctx: &IOContext,
    ) -> Result<()> {
        let data_file = segment_file_name(&si.name, "", DATA_EXTENSION);
        let entries_file = segment_file_name(&si.name, "", ENTRIES_EXTENSION);

        let mut data = dir.create_output(&data_file, io_ctx)?;
        let mut entries = dir.create_output(&entries_file, io_ctx)?;

        codec_util::write_index_header(&mut data, DATA_CODEC, VERSION_CURRENT, si.get_id(), "")?;
        codec_util::write_index_header(
            &mut entries,
            ENTRY_CODEC,
            VERSION_CURRENT,
            si.get_id(),
            "",
        )?;

        // write number of files
        entries.write_vint(si.files().len() as i32)?;
        for file in si.files() {
            // write bytes for file
            let start_offset = data.file_pointer();

            let mut input = dir.open_checksum_input(file, &IOContext::READ_ONCE)?;

            // just copies the index header, verifying that its id matches what we expect
            codec_util::verify_and_copy_index_header(&mut input, &mut data, si.get_id())?;

            // copy all bytes except the footer
            let num_bytes_to_copy =
                input.len() as usize - codec_util::footer_length() - input.file_pointer() as usize;
            data.copy_bytes(&mut input, num_bytes_to_copy)?;

            // verify footer (checksum) matches for the incoming file we are copying
            let checksum = codec_util::check_footer(&mut input)?;

            // this is poached from codec_util::write_footer, be we need to use our own checksum
            // not data.checksum(), but I think adding a public method to codec_util to do that
            // is somewhat dangerous:
            data.write_int(codec_util::FOOTER_MAGIC)?;
            data.write_int(0)?;
            data.write_long(checksum)?;

            let end_offset = data.file_pointer();
            let length = end_offset - start_offset;

            // write entry for file
            entries.write_string(strip_segment_name(file))?;
            entries.write_long(start_offset)?;
            entries.write_long(length)?;
        }

        codec_util::write_footer(&mut data)?;
        codec_util::write_footer(&mut entries)
    }
}

/// Offset/Length for a slice inside of a compound file */
#[derive(Debug)]
struct FileEntry(i64, i64);

/// Class for accessing a compound stream.
/// This class implements a directory, but is limited to only read operations.
/// Directory methods that would normally modify data throw an exception.
pub struct Lucene50CompoundReader<D: Directory> {
    directory: Arc<D>,
    name: String,
    entries: HashMap<String, FileEntry>,
    input: Box<dyn IndexInput>,
    _version: i32,
}

impl<D: Directory> Lucene50CompoundReader<D> {
    /// Create a new CompoundFileDirectory.
    // TODO: we should just pre-strip "entries" and append segment name up-front like simpletext?
    // this need not be a "general purpose" directory anymore (it only writes index files)
    pub fn new<C: Codec>(
        directory: Arc<D>,
        si: &SegmentInfo<D, C>,
        context: &IOContext,
    ) -> Result<Lucene50CompoundReader<D>> {
        let data_file_name = segment_file_name(si.name.as_ref(), "", DATA_EXTENSION);
        let entries_file_name = segment_file_name(si.name.as_ref(), "", ENTRIES_EXTENSION);
        let (version, entries) =
            Lucene50CompoundReader::read_entries(si.id.as_ref(), &*directory, &entries_file_name)?;
        let mut expected_length = codec_util::index_header_length(DATA_CODEC, "") as u64;
        for v in entries.values() {
            expected_length += v.1 as u64; // 1 for length
        }
        expected_length += codec_util::footer_length() as u64;

        let mut input = directory.open_input(&data_file_name, context)?;
        codec_util::check_index_header(
            input.as_mut(),
            DATA_CODEC,
            version,
            version,
            si.id.as_ref(),
            "",
        )?;
        codec_util::retrieve_checksum(input.as_mut())?;
        if input.as_ref().len() != expected_length {
            return Err(format!(
                "length should be {} bytes, but is {} instead",
                expected_length,
                input.as_ref().len()
            )
            .into());
        }
        Ok(Lucene50CompoundReader {
            directory,
            name: si.name.clone(),
            entries,
            input,
            _version: version,
        })
    }

    /// Helper method that reads CFS entries from an input stream
    fn read_entries(
        segment_id: &[u8],
        directory: &D,
        entries_file_name: &str,
    ) -> Result<(i32, HashMap<String, FileEntry>)> {
        let mut entries_stream =
            directory.open_checksum_input(entries_file_name, &IOContext::READ_ONCE)?;
        let version = codec_util::check_index_header(
            &mut entries_stream,
            ENTRY_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            segment_id,
            "",
        )?;
        let num_entries = entries_stream.read_vint()?;
        let mut mappings = HashMap::with_capacity(num_entries as usize);
        for _ in 0..num_entries {
            let id = entries_stream.read_string()?;
            let offset = entries_stream.read_long()?;
            let length = entries_stream.read_long()?;
            let previous = mappings.insert(id.clone(), FileEntry(offset, length));
            if previous.is_some() {
                return Err(format!("Duplicate cfs entry id={} in CFS", id).into());
            }
        }

        codec_util::check_footer(&mut entries_stream)?;
        Ok((version, mappings))
    }
}

impl<D: Directory> Directory for Lucene50CompoundReader<D> {
    type IndexOutput = D::IndexOutput;
    type TempOutput = D::TempOutput;

    /// Returns an array of strings, one for each file in the directory.
    fn list_all(&self) -> Result<Vec<String>> {
        Ok(self
            .entries
            .keys()
            .map(|n| format!("{}{}", self.name, n))
            .collect())
    }

    /// Returns the length of a file in the directory.
    /// @throws IOException if the file does not exist */
    fn file_length(&self, name: &str) -> Result<i64> {
        self.entries
            .get(strip_segment_name(name))
            .map(|e| e.1)
            .ok_or_else(|| "File not Found".into())
    }

    fn create_output(&self, _name: &str, _ctx: &IOContext) -> Result<Self::IndexOutput> {
        unimplemented!()
    }

    fn open_input(&self, name: &str, _context: &IOContext) -> Result<Box<dyn IndexInput>> {
        let id = strip_segment_name(name);
        let entry = self.entries.get(id).ok_or_else(|| {
            let file_name = segment_file_name(&self.name, "", DATA_EXTENSION);
            format!(
                "No sub-file with id {} found in compound file \"{}\" (fileName={} files: {:?})",
                id,
                file_name,
                name,
                self.entries.keys()
            )
        })?;
        self.input.slice(name, entry.0, entry.1)
    }

    fn create_temp_output(
        &self,
        _prefix: &str,
        _suffix: &str,
        _ctx: &IOContext,
    ) -> Result<Self::TempOutput> {
        unimplemented!();
    }

    fn delete_file(&self, _name: &str) -> Result<()> {
        unimplemented!();
    }

    fn sync(&self, _name: &HashSet<String>) -> Result<()> {
        bail!(ErrorKind::UnsupportedOperation(Cow::Borrowed("")))
    }

    fn sync_meta_data(&self) -> Result<()> {
        Ok(())
    }

    fn rename(&self, _source: &str, _dest: &str) -> Result<()> {
        unimplemented!()
    }
}

impl<D: Directory> fmt::Display for Lucene50CompoundReader<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Lucene50CompoundReader({})", self.directory)
    }
}
