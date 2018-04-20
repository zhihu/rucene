use error::*;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use core::codec::codec_util;
use core::codec::format::CompoundFormat;
use core::store::IndexInput;
use core::store::{Directory, DirectoryRc};
use core::store::{IOContext, IO_CONTEXT_READONCE};

use core::index::{segment_file_name, strip_segment_name, SegmentInfo};

const DATA_EXTENSION: &str = "cfs";
/// Extension of compound file entries */
pub const ENTRIES_EXTENSION: &str = "cfe";
pub const DATA_CODEC: &str = "Lucene50CompoundData";
pub const ENTRY_CODEC: &str = "Lucene50CompoundEntries";
pub const VERSION_START: i32 = 0;
pub const VERSION_CURRENT: i32 = VERSION_START;

pub struct Lucene50CompoundFormat {}

impl Lucene50CompoundFormat {}

impl CompoundFormat for Lucene50CompoundFormat {
    fn get_compound_reader(
        &self,
        dir: DirectoryRc,
        si: &SegmentInfo,
        ioctx: &IOContext,
    ) -> Result<DirectoryRc> {
        let reader = Lucene50CompoundReader::new(dir.clone(), si, ioctx)?;
        Ok(Arc::new(reader))
    }

    fn write(&self, _dir: DirectoryRc, _si: &SegmentInfo, _ioctx: &IOContext) -> Result<()> {
        unimplemented!();
    }
}

/// Offset/Length for a slice inside of a compound file */
#[derive(Debug)]
pub struct FileEntry(i64, i64);

/// Class for accessing a compound stream.
/// This class implements a directory, but is limited to only read operations.
/// Directory methods that would normally modify data throw an exception.
pub struct Lucene50CompoundReader {
    pub directory: DirectoryRc,
    name: String,
    entries: HashMap<String, FileEntry>,
    input: Box<IndexInput>,
    pub version: i32,
}

impl Lucene50CompoundReader {
    /// Create a new CompoundFileDirectory.
    // TODO: we should just pre-strip "entries" and append segment name up-front like simpletext?
    // this need not be a "general purpose" directory anymore (it only writes index files)
    pub fn new(
        directory: DirectoryRc,
        si: &SegmentInfo,
        context: &IOContext,
    ) -> Result<Lucene50CompoundReader> {
        let data_file_name = segment_file_name(si.name.as_ref(), "", DATA_EXTENSION);
        let entries_file_name = segment_file_name(si.name.as_ref(), "", ENTRIES_EXTENSION);
        let (version, entries) =
            Lucene50CompoundReader::read_entries(si.id.as_ref(), &directory, &entries_file_name)?;
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
            ).into());
        }
        Ok(Lucene50CompoundReader {
            directory,
            name: si.name.clone(),
            entries,
            input,
            version,
        })
    }

    /// Helper method that reads CFS entries from an input stream
    pub fn read_entries(
        segment_id: &[u8],
        directory: &DirectoryRc,
        entries_file_name: &str,
    ) -> Result<(i32, HashMap<String, FileEntry>)> {
        let mut entries_stream =
            directory.open_checksum_input(entries_file_name, &IO_CONTEXT_READONCE)?;
        let version = codec_util::check_index_header(
            entries_stream.as_mut(),
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

        codec_util::check_footer(entries_stream.as_mut())?;
        Ok((version, mappings))
    }
}

impl Directory for Lucene50CompoundReader {
    fn open_input(&self, name: &str, _context: &IOContext) -> Result<Box<IndexInput>> {
        let id = strip_segment_name(name);
        let entry = self.entries.get(&id).ok_or_else(|| {
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

    /// Returns an array of strings, one for each file in the directory.
    fn list_all(&self) -> Result<Vec<String>> {
        Ok(self.entries
            .keys()
            .map(|n| format!("{}{}", self.name, n))
            .collect())
    }

    /// Returns the length of a file in the directory.
    /// @throws IOException if the file does not exist */
    fn file_length(&self, name: &str) -> Result<i64> {
        self.entries
            .get(&strip_segment_name(name))
            .map(|e| e.1)
            .ok_or_else(|| "File not Found".into())
    }
}

impl Drop for Lucene50CompoundReader {
    fn drop(&mut self) {}
}

impl fmt::Display for Lucene50CompoundReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Lucene50CompoundReader({})", self.directory)
    }
}
