extern crate byteorder;
extern crate num_cpus;
extern crate rucene;

use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::io;
use std::io::*;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::*;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;

use byteorder::{BigEndian, ReadBytesExt};

use rucene::core::index::index_lookup::*;
use rucene::core::index::point_values::*;
use rucene::core::index::{DocValuesType, FieldInfo, Fieldable, IndexReader, LeafReader,
                          StandardDirectoryReader};
use rucene::core::search::collector::top_docs::*;
use rucene::core::search::query_string::*;
use rucene::core::search::searcher::*;
use rucene::core::store::MmapDirectory;
use rucene::core::util::{DocId, VariantValue};
use rucene::error::Result as RuceneResult;

fn read_int(read: &mut Read) -> Result<i32> {
    read.read_i32::<BigEndian>()
}

fn read_byte(read: &mut Read) -> Result<u8> {
    let mut b = [0 as u8; 1];
    read.read_exact(&mut b)?;
    Ok(b[0])
}

fn read_string(read: &mut Read) -> Result<String> {
    let s = String::from_utf8(read_binary(read)?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(s)
}

fn read_binary(read: &mut Read) -> Result<Vec<u8>> {
    let length = read_int(read)? as usize;
    let mut buffer = vec![0 as u8; length];
    read.read_exact(&mut buffer)?;
    Ok(buffer)
}

fn read_double(read: &mut Read) -> Result<f64> {
    read.read_f64::<BigEndian>()
}

fn read_float(read: &mut Read) -> Result<f32> {
    read.read_f32::<BigEndian>()
}

fn read_long(read: &mut Read) -> Result<i64> {
    read.read_i64::<BigEndian>()
}

fn new_error<T>(s: &str) -> Result<T> {
    Err(io::Error::new(io::ErrorKind::Other, s))
}

#[derive(Debug)]
enum VerifyField {
    StoredBinary(Vec<u8>),
    StoredString(String),
    StoredDouble(f64),
    StoredFloat(f32),
    StoredInt(i32),
    StoredLong(i64),
}

#[derive(Debug)]
struct VerifyTask {
    query: String,
    documents: HashMap<DocId, HashMap<String, Vec<VerifyField>>>,
    positions: HashMap<DocId, HashMap<String, Vec<(i32, i32, i32)>>>,
}

fn read_stored_field(read: &mut Read) -> Result<Option<(String, VerifyField)>> {
    let mut t = [0 as u8; 1];
    read.read_exact(&mut t)?;
    let t = t[0];
    let field = read_string(read)?;
    let value = match t {
        0 => VerifyField::StoredBinary(read_binary(read)?),
        1 => VerifyField::StoredBinary(read_binary(read)?),
        2 => VerifyField::StoredDouble(read_double(read)?),
        3 => VerifyField::StoredFloat(read_float(read)?),
        4 => VerifyField::StoredInt(read_int(read)?),
        5 => VerifyField::StoredLong(read_long(read)?),
        6 => VerifyField::StoredString(read_string(read)?),
        _ => panic!("Unknown field type"),
    };

    if t == 0 {
        Ok(None)
    } else {
        Ok(Some((field, value)))
    }
}

fn read_stored_fields(read: &mut Read) -> Result<HashMap<String, Vec<VerifyField>>> {
    let mut fields = HashMap::new();
    loop {
        if let Some((field, value)) = read_stored_field(read)? {
            if !fields.contains_key(&field) {
                fields.insert(field.clone(), vec![]);
            }
            let values = fields.get_mut(&field).unwrap();
            values.push(value);
        } else {
            break;
        }
    }
    Ok(fields)
}

fn read_positions(read: &mut Read) -> Result<HashMap<String, Vec<(i32, i32, i32)>>> {
    let mut positions = HashMap::new();
    loop {
        if read_byte(read)? == 0 {
            break;
        }
        let term = read_string(read)?;
        let freq = read_int(read)?;
        let mut term_positions = Vec::new();
        for _ in 0..freq {
            let position = read_int(read)?;
            let start_offset = read_int(read)?;
            let end_offset = read_int(read)?;
            term_positions.push((position, start_offset, end_offset));
        }
        positions.insert(term, term_positions);
    }
    Ok(positions)
}

fn read_doc_id(read: &mut Read) -> Result<(i32, i32)> {
    let base = read_int(read)?;
    let doc = read_int(read)?;
    Ok((base, doc))
}

fn read_all_docs(
    read: &mut Read,
) -> Result<(
    HashMap<DocId, HashMap<String, Vec<VerifyField>>>,
    HashMap<DocId, HashMap<String, Vec<(i32, i32, i32)>>>,
)> {
    let mut docs = HashMap::new();
    let mut positions = HashMap::new();
    loop {
        let (base, doc) = read_doc_id(read)?;
        if base >= 0 && doc >= 0 {
            if read_byte(read)? == 1 {
                positions.insert(base + doc, read_positions(read)?);
            }
            docs.insert(base + doc, read_stored_fields(read)?);
        } else {
            break;
        }
    }
    Ok((docs, positions))
}

fn read_task(read: &mut Read) -> Result<VerifyTask> {
    let query = read_string(read)?;
    let (documents, positions) = read_all_docs(read)?;
    Ok(VerifyTask {
        query,
        documents,
        positions,
    })
}

fn convert_rucene_result<T>(from: rucene::error::Result<T>, error: &str) -> Result<T> {
    if from.is_err() {
        eprintln!("ERROR: {}\n{:?}", error, from.as_ref().err());
    }
    from.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn verify_doc_positions(
    reader: Arc<StandardDirectoryReader>,
    term_positions: &mut HashMap<String, Vec<(i32, i32, i32)>>,
    field: &str,
    doc_id: DocId,
    output: &mut String,
) -> Result<()> {
    let leaf_reader = reader.leaf_reader_for_doc(doc_id);
    let mut index_lookup = LeafIndexLookup::new(convert_rucene_result(
        leaf_reader.fields(),
        "Failed to get fields",
    )?);
    convert_rucene_result(
        index_lookup.set_document(doc_id - leaf_reader.doc_base()),
        "Can not set document for index lookup",
    )?;

    let index_field_ref = index_lookup.get(field);
    for (ref mut term, ref mut positions) in term_positions {
        let index_field_term_ref = convert_rucene_result(
            index_field_ref.get_with_flags(term, FLAG_POSITIONS | FLAG_OFFSETS),
            "Can not get index field term",
        )?;
        convert_rucene_result(
            index_field_term_ref.reset(),
            "Can not reset index field term",
        )?;
        let tf = index_field_term_ref.tf();
        if tf != positions.len() as i32 {
            output.push_str(&format!(
                "Term frequency mismatch for term '{}' in doc {}: {} vs {}",
                term,
                doc_id,
                tf,
                positions.len()
            ));
            eprintln!(
                "Verify positions of term '{}' in doc {} failed, term frequency mismatch: {} vs {}",
                term,
                doc_id,
                tf,
                positions.len()
            );
            return new_error("Term frequency mismatch");
        }
        let index = 0usize;
        while index_field_term_ref.has_next() {
            let (lp, ls, le) = positions[index];
            let tp =
                convert_rucene_result(index_field_term_ref.next_pos(), "Can not get next term")?;
            if lp != tp.position {
                output.push_str(&format!(
                    "Term Position Mismatch: {} vs {}",
                    lp, tp.position
                ));
                return new_error("Term Position Mismatch");
            }
            if ls != tp.start_offset {
                output.push_str(&format!(
                    "Term Start Offset Mismatch: {} vs {}",
                    ls, tp.start_offset
                ));
                return new_error("Term Start Offset Mismatch");
            }
            if le != tp.end_offset {
                output.push_str(&format!(
                    "Term End Offset Mismatch: {} vs {}",
                    ls, tp.end_offset
                ));
                return new_error("Term End Offset Mismatch");
            }
        }
    }
    Ok(())
}

fn verify_stored_fields(
    reader: Arc<StandardDirectoryReader>,
    fields: &mut HashMap<String, Vec<VerifyField>>,
    doc: DocId,
    output: &mut String,
) -> Result<()> {
    let names: Vec<String> = fields.keys().map(|n| n.clone()).collect();
    let document = reader
        .document(doc, &names)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Could not read stored fields"))?;
    for field in document.fields.iter() {
        let field = &field.field;
        let mut finished = false;
        {
            let stored_values = fields.get_mut(field.name());
            if stored_values.is_none() {
                output.push_str(&format!(
                    "Rucene is missing stored field: {} for document {}\n",
                    field.name(),
                    doc
                ));
                continue;
            }
            let stored_values = stored_values.unwrap();
            let fields_data = field.fields_data();
            let mut remove = std::usize::MAX;
            let stored_values_num = stored_values.len();
            for i in 0..stored_values_num {
                let value = &stored_values[i];
                if match value {
                    &VerifyField::StoredBinary(ref binary) => {
                        if let &VariantValue::Binary(ref rbinary) = fields_data {
                            rbinary == binary
                        } else {
                            false
                        }
                    }
                    &VerifyField::StoredString(ref string) => {
                        if let &VariantValue::VString(ref rstring) = fields_data {
                            rstring == string
                        } else {
                            false
                        }
                    }
                    &VerifyField::StoredDouble(ref double) => {
                        if let &VariantValue::Double(ref rdouble) = fields_data {
                            rdouble == double
                        } else {
                            false
                        }
                    }
                    &VerifyField::StoredFloat(ref float) => {
                        if let &VariantValue::Float(ref rfloat) = fields_data {
                            rfloat == float
                        } else {
                            false
                        }
                    }
                    &VerifyField::StoredInt(ref int) => {
                        if let &VariantValue::Int(ref rint) = fields_data {
                            rint == int
                        } else {
                            false
                        }
                    }
                    &VerifyField::StoredLong(ref long) => {
                        if let &VariantValue::Long(ref rlong) = fields_data {
                            rlong == long
                        } else {
                            false
                        }
                    }
                } {
                    remove = i;
                    break;
                }
            }
            if remove == std::usize::MAX {
                output.push_str(&format!(
                    "Can not find stored field {}/{:?} in rucene for document {}\n",
                    field.name(),
                    fields_data,
                    doc,
                ));
            } else {
                stored_values.remove(remove);
                finished = stored_values.is_empty();
            }
        }
        if finished {
            fields.remove(field.name());
        }
    }
    if !fields.is_empty() {
        output.push_str(&format!("Rucene has more stored fields: {:?}\n", fields));
    }
    Ok(())
}

fn verify_against_lucene_with(
    reader: Arc<StandardDirectoryReader>,
    searcher: &IndexSearcher,
    field: String,
    task: VerifyTask,
) -> Result<()> {
    let s = task.query.trim();
    if s.is_empty() {
        return Ok(());
    }
    let mut docs = task.documents;
    let top_docs = {
        let query = QueryStringQueryBuilder::new(s.into(), vec![(field.clone(), 1.0)], 1, 1.0)
            .build()
            .unwrap();
        let mut collector = TopDocsCollector::new(docs.len());
        searcher
            .search_parallel(query.as_ref(), &mut collector)
            .unwrap();
        collector.top_docs()
    };
    let rucene_docs: Vec<DocId> = top_docs.score_docs().iter().map(|x| x.doc_id()).collect();
    let mut output = String::new();
    if rucene_docs.len() != docs.len() {
        output.push_str(&format!(
            "Total hits Rucene vs Lucene: {} vs {}\nLucene: {:?}\nRucene: {:?}\n",
            top_docs.total_hits(),
            docs.len(),
            docs,
            rucene_docs
        ));
    }
    if rucene_docs.is_empty() && top_docs.total_hits() > 0 {
        output.push_str(&format!(
            "No doc returned when total hits is {}\n",
            top_docs.total_hits()
        ));
    }
    let mut positions = task.positions;
    for score_doc in rucene_docs {
        if let Some(mut fields) = docs.remove(&score_doc) {
            verify_stored_fields(reader.clone(), &mut fields, score_doc, &mut output)?;
        } else {
            output.push_str(&format!("Lucene is missing doc: {}\n", score_doc));
        }
        if let Some(mut positions) = positions.remove(&score_doc) {
            verify_doc_positions(
                reader.clone(),
                &mut positions,
                &field,
                score_doc,
                &mut output,
            )?;
        } else {
            output.push_str(&format!("Lucene is missing doc: {}\n", score_doc));
        }
    }
    if !docs.is_empty() {
        output.push_str(&format!("Rucene is missing docs: {:?}\n", docs));
    }

    if !output.is_empty() {
        panic!(format!("Inconsistent Query: '{}'\n{}", s, output));
    }

    Ok(())
}

fn verify_binary_doc_values(
    leaf_reader: &LeafReader,
    input: &mut Read,
    max_doc: i32,
    field_name: &str,
    field_number: i32,
) -> Result<()> {
    eprintln!(
        "Verify binary doc values for field {}@{}",
        field_name, field_number
    );
    let binary_doc_values = convert_rucene_result(
        leaf_reader.get_binary_doc_values(field_name),
        "failed to get binary doc values",
    )?;
    for doc_id in 0..max_doc {
        let expected = read_binary(input)?;
        let rucene = convert_rucene_result(
            binary_doc_values.get(doc_id),
            "failed to get binary doc values for doc",
        )?;
        if expected != rucene {
            eprintln!(
                "Doc value of document {} mismatch. '{:?}' vs '{:?}'",
                doc_id, expected, rucene
            );
            return new_error("Binary doc values mismatch");
        }
    }
    Ok(())
}

fn verify_numeric_doc_values(
    leaf_reader: &LeafReader,
    input: &mut Read,
    max_doc: i32,
    field_name: &str,
    field_number: i32,
) -> Result<()> {
    eprintln!(
        "Verify numeric doc values for field {}@{}",
        field_name, field_number
    );
    let numeric_doc_values = convert_rucene_result(
        leaf_reader.get_numeric_doc_values(field_name),
        "failed to get numeric doc values",
    )?;
    for doc_id in 0..max_doc {
        let expected = read_long(input)?;
        let rucene = convert_rucene_result(
            numeric_doc_values.get(doc_id),
            "failed to get numeric doc values for doc",
        )?;
        if expected != rucene {
            eprintln!(
                "Doc value of document {} mismatch. '{:?}' vs '{:?}'",
                doc_id, expected, rucene
            );
            return new_error("numeric doc values mismatch");
        }
    }
    Ok(())
}

fn verify_sorted_doc_values(
    leaf_reader: &LeafReader,
    input: &mut Read,
    max_doc: i32,
    field_name: &str,
    field_number: i32,
) -> Result<()> {
    eprintln!(
        "Verify sorted doc values for field {}@{}",
        field_name, field_number
    );
    let sorted_doc_values = convert_rucene_result(
        leaf_reader.get_sorted_doc_values(field_name),
        "failed to get sorted doc values",
    )?;
    for doc_id in 0..max_doc {
        let expected = read_binary(input)?;
        let rucene = convert_rucene_result(
            sorted_doc_values.get(doc_id),
            "failed to get sorted doc values for doc",
        )?;
        if expected != rucene {
            eprintln!(
                "Doc value of document {} mismatch. '{:?}' vs '{:?}'",
                doc_id, expected, rucene
            );
            return new_error("sorted doc values mismatch");
        }
    }
    Ok(())
}

fn verify_sorted_numeric_doc_values(
    leaf_reader: &LeafReader,
    input: &mut Read,
    max_doc: i32,
    field_name: &str,
    field_number: i32,
) -> Result<()> {
    eprintln!(
        "Verify sorted numeric doc values for field {}@{}",
        field_name, field_number
    );
    let sorted_numeric_doc_values = convert_rucene_result(
        leaf_reader.get_sorted_numeric_doc_values(field_name),
        "failed to get sorted_numeric doc values",
    )?;
    let mut context = None;
    for doc_id in 0..max_doc {
        let ctx = convert_rucene_result(
            sorted_numeric_doc_values.set_document(context, doc_id),
            "failed to set document id",
        )?;
        let count = read_int(input)? as usize;
        if sorted_numeric_doc_values.count(&ctx) != count {
            eprintln!(
                "Sorted numeric doc values count mismatch {} vs {}",
                count,
                sorted_numeric_doc_values.count(&ctx)
            );
            return new_error("");
        }
        for vi in 0..count {
            let expected = read_long(input)?;
            let rucene = convert_rucene_result(
                sorted_numeric_doc_values.value_at(&ctx, vi),
                "failed to get sorted numeric doc values for doc",
            )?;
            if expected != rucene {
                eprintln!(
                    "Sorted numeric doc value of document {} at {} mismatch. '{:?}' vs '{:?}'",
                    doc_id, vi, expected, rucene
                );
                return new_error("sorted numeric doc values mismatch");
            }
        }
        context = Some(ctx)
    }
    Ok(())
}

fn verify_sorted_set_doc_values(
    leaf_reader: &LeafReader,
    input: &mut Read,
    max_doc: i32,
    field_name: &str,
    field_number: i32,
) -> Result<()> {
    eprintln!(
        "Verify sorted set doc values for field {}@{}",
        field_name, field_number
    );
    let sorted_set_doc_values = convert_rucene_result(
        leaf_reader.get_sorted_set_doc_values(field_name),
        "failed to get sorted_set doc values",
    )?;
    for doc_id in 0..max_doc {
        let mut ctx = convert_rucene_result(
            sorted_set_doc_values.set_document(doc_id),
            "failed to set document id",
        )?;
        loop {
            let ord = read_long(input)?;
            let rucene_ord = convert_rucene_result(
                sorted_set_doc_values.next_ord(&mut ctx),
                "Failed to get next ord of sorted set doc values",
            )?;
            if rucene_ord != ord {
                eprintln!(
                    "Sorted set doc value ordinal of document {} mismatch. '{:?}' vs '{:?}'",
                    doc_id, ord, rucene_ord
                );
                return new_error("sorted set doc values ordinal mismatch");
            }
            if ord != -1 {
                let expected = read_binary(input)?;
                let rucene = convert_rucene_result(
                    sorted_set_doc_values.lookup_ord(rucene_ord),
                    "Failed to lookup ord data",
                )?;
                if expected != rucene {
                    eprintln!(
                        "Sorted set doc value of document {} at {} mismatch. '{:?}' vs '{:?}'",
                        doc_id, ord, expected, rucene
                    );
                    return new_error("sorted set doc values mismatch");
                }
            } else {
                break;
            }
        }
    }
    Ok(())
}

fn verify_field_field<T>(lhs: &T, rhs: &T, name: &str) -> Result<()>
where
    T: PartialEq + Debug,
{
    if lhs != rhs {
        eprintln!(
            "Field {} mismatch, expecting '{:?}', got {:?}",
            name, lhs, rhs
        );
        return new_error("field mismatch");
    }
    Ok(())
}

fn verify_field_info(
    lhs: Arc<FieldInfo>,
    rhs: Arc<FieldInfo>,
    name: &str,
    number: i32,
) -> Result<()> {
    if lhs.name != name {
        eprintln!(
            "Field name mismatch, expecting field name '{}', got {}",
            name, lhs.name
        );
        return new_error("field name mismatch");
    }
    if lhs.number != number {
        eprintln!(
            "Field number mismatch, expecting field number '{}', got {}",
            number, lhs.number
        );
        return new_error("field number mismatch");
    }
    verify_field_field(
        &lhs.doc_values_type,
        &rhs.doc_values_type,
        "doc_values_type",
    )?;
    verify_field_field(
        &lhs.has_store_term_vector,
        &rhs.has_store_term_vector,
        "has_store_term_vector",
    )?;
    verify_field_field(&lhs.omit_norms, &rhs.omit_norms, "omit_norms")?;
    verify_field_field(&lhs.index_options, &rhs.index_options, "index_options")?;
    verify_field_field(
        &lhs.has_store_payloads,
        &rhs.has_store_payloads,
        "has_store_payloads",
    )?;
    verify_field_field(&lhs.attributes, &rhs.attributes, "attributes")?;
    verify_field_field(&lhs.dv_gen, &rhs.dv_gen, "dv_gen")?;
    verify_field_field(
        &lhs.point_dimension_count,
        &rhs.point_dimension_count,
        "point_dimension_count",
    )?;
    verify_field_field(
        &lhs.point_num_bytes,
        &rhs.point_num_bytes,
        "point_num_bytes",
    )?;
    Ok(())
}

fn verify_doc_values_against_lucene(
    reader: Arc<StandardDirectoryReader>,
    input: &mut Read,
) -> Result<()> {
    let leaves = read_int(input)?;
    eprintln!("There are {} leaf readers to verify", leaves);
    let leaf_readers = reader.leaves();
    for i in 0..leaves {
        let leaf_reader = leaf_readers[i as usize];
        let max_doc = read_int(input)?;
        if leaf_reader.max_doc() != max_doc {
            eprintln!(
                "Max docs of leaf reader {} doesn't match. {} vs {}",
                i,
                max_doc,
                leaf_reader.max_doc()
            );
            return new_error("Max docs mismatch");
        }
        let fields_count = read_int(input)? as usize;
        let field_infos = leaf_reader.field_infos();
        if fields_count != field_infos.by_number.len() {
            eprintln!(
                "Fields count doesn't match for leaf reader {}. {} vs {}",
                i,
                fields_count,
                leaf_reader.field_infos().by_number.len()
            );
            return new_error("Number of fields mismatch");
        }
        for _ in 0..fields_count {
            let field_number = read_int(input)?;
            let field_name = read_string(input)?;
            let field_info;
            if let Some(field_info_by_number) = field_infos.by_number.get(&field_number) {
                if let Some(field_info_by_name) = field_infos.by_name.get(&field_name) {
                    verify_field_info(
                        Arc::clone(field_info_by_number),
                        Arc::clone(field_info_by_name),
                        &field_name,
                        field_number,
                    )?;
                    field_info = Arc::clone(field_info_by_number);
                } else {
                    eprintln!(
                        "Can't find field '{}' by name for leaf reader {}",
                        field_name, i,
                    );
                    return new_error("Can't find field by number");
                }
            } else {
                eprintln!(
                    "Can't find field '{}' by number for leaf reader {}",
                    field_number, i,
                );
                return new_error("Can't find field by number");
            }
            match read_byte(input)? {
                0 => {
                    if let DocValuesType::Binary = field_info.doc_values_type {
                        verify_binary_doc_values(
                            leaf_reader,
                            input,
                            max_doc,
                            &field_name,
                            field_number,
                        )
                    } else {
                        new_error("Unexpected binary doc values")
                    }
                }
                1 => {
                    if let DocValuesType::Null = field_info.doc_values_type {
                        Ok(())
                    } else {
                        new_error("Unexpected null doc values")
                    }
                }
                2 => {
                    if let DocValuesType::Numeric = field_info.doc_values_type {
                        verify_numeric_doc_values(
                            leaf_reader,
                            input,
                            max_doc,
                            &field_name,
                            field_number,
                        )
                    } else {
                        new_error("Unexpected numeric doc values")
                    }
                }
                3 => {
                    if let DocValuesType::Sorted = field_info.doc_values_type {
                        verify_sorted_doc_values(
                            leaf_reader,
                            input,
                            max_doc,
                            &field_name,
                            field_number,
                        )
                    } else {
                        new_error("Unexpected sorted doc values")
                    }
                }
                4 => {
                    if let DocValuesType::SortedNumeric = field_info.doc_values_type {
                        verify_sorted_numeric_doc_values(
                            leaf_reader,
                            input,
                            max_doc,
                            &field_name,
                            field_number,
                        )
                    } else {
                        new_error("Unexpected sorted numeric doc values")
                    }
                }
                5 => {
                    if let DocValuesType::SortedSet = field_info.doc_values_type {
                        verify_sorted_set_doc_values(
                            leaf_reader,
                            input,
                            max_doc,
                            &field_name,
                            field_number,
                        )
                    } else {
                        new_error("Unexpected sorted set doc values")
                    }
                }
                _ => new_error("Unknown field type"),
            }?
        }
    }

    Ok(())
}

struct PointValuesVerifier<'a> {
    input: &'a mut Read,
    verified: i64,
}

impl<'a> IntersectVisitor for PointValuesVerifier<'a> {
    fn visit(&mut self, _doc_id: DocId) -> RuceneResult<()> {
        Ok(())
    }

    fn visit_by_packed_value(&mut self, doc_id: DocId, packed_value: &[u8]) -> RuceneResult<()> {
        let expected_doc_id = read_int(self.input)?;
        let expected_binary = read_binary(self.input)?;
        let expected_binary: &[u8] = &expected_binary;
        if expected_doc_id != doc_id {
            let msg = format!(
                "PointValues doc mismatch: expecting {} got {} instead",
                expected_doc_id, doc_id
            );
            panic!(msg);
        }
        if packed_value != expected_binary {
            let msg = format!(
                "PointValues packed_value mismatch: expecting {:?} got {:?} instead",
                expected_binary, packed_value
            );
            panic!(msg);
        }
        self.verified += 1;
        Ok(())
    }

    fn compare(&self, _min_packed_value: &[u8], _max_packed_value: &[u8]) -> Relation {
        Relation::CellCrossesQuery
    }
}

fn verify_point_values_against_lucene(
    reader: Arc<StandardDirectoryReader>,
    input: &mut Read,
) -> Result<()> {
    let num_point_fields = read_int(input)?;
    if num_point_fields == 0 {
        return Ok(());
    }
    let num_leaves = read_int(input)?;
    eprintln!("Verifying {} fields with point values", num_point_fields);
    let empty = vec![0u8; 0];
    for _ in 0..num_point_fields {
        let field_name = read_string(input)?;
        let leaf_readers = reader.leaves();
        let mut verifier = PointValuesVerifier { input, verified: 0 };
        for i in 0..num_leaves {
            let leaf_reader = leaf_readers[i as usize];
            if let Some(point_values) = leaf_reader.point_values() {
                if leaf_reader
                    .field_infos()
                    .field_info_by_name(&field_name)
                    .is_some()
                {
                    convert_rucene_result(
                        point_values.intersect(&field_name, &mut verifier),
                        "failed to intersect point values",
                    )?;
                }
            } else {
                return new_error(&format!("Missing point values field {}", &field_name));
            }
            convert_rucene_result(
                verifier.visit_by_packed_value(-1, &empty),
                "failed to visit packed value",
            )?;
        }
        eprintln!(
            "Verified {} point values for field '{}'",
            verifier.verified, field_name
        );
    }
    Ok(())
}

fn verify_against_lucene(index: String, field: String) -> Result<()> {
    let directory = Arc::new(MmapDirectory::new(&Path::new(&index), 1024 * 1024).unwrap());
    let reader = Arc::new(StandardDirectoryReader::open(directory).unwrap());
    let mut searcher = IndexSearcher::new(reader.clone());
    searcher.with_thread_pool(16);
    let searcher = Arc::new(searcher);
    let mut input = stdin();
    eprintln!("Verifying doc values of index '{}'", index);
    verify_doc_values_against_lucene(reader.clone(), &mut input)?;
    eprintln!("Verifying point values if there is any");
    verify_point_values_against_lucene(reader.clone(), &mut input)?;
    eprintln!(
        "Verifying queries against field '{}' of index '{}'",
        field, index
    );
    let num_threads = num_cpus::get();
    let verified = Arc::new(AtomicUsize::new(0));
    let mut worker_index = 0;
    let mut senders = Vec::new();
    let show_progress_time = Arc::new(Mutex::new(time::Instant::now()));
    let threads: Vec<thread::JoinHandle<_>> = (0..num_threads)
        .map(|_| {
            let field = field.clone();
            let reader = reader.clone();
            let searcher = searcher.clone();
            let verified = verified.clone();
            let show_progress_time = show_progress_time.clone();
            let (sender, receiver) = sync_channel(128);
            senders.push(sender);
            thread::spawn(move || loop {
                if let Ok(task) = receiver.recv() {
                    if verify_against_lucene_with(
                        reader.clone(),
                        searcher.as_ref(),
                        field.clone(),
                        task,
                    ).is_ok()
                    {
                        let verified = verified.fetch_add(1, Ordering::SeqCst);
                        if verified % 10 == 0 {
                            let mut time = show_progress_time.lock().unwrap();
                            let elapsed = time.elapsed();
                            eprintln!(
                                "10 more out of {} queries verified in {} ms",
                                verified,
                                ((elapsed.subsec_nanos() as u64 / 1000)
                                    + (elapsed.as_secs() * 1000000))
                                    as f64 / 1000.0
                            );
                            *time = time::Instant::now();
                        }
                        continue;
                    }
                } else {
                    break;
                }
            })
        })
        .collect();
    loop {
        match read_task(&mut input) {
            Ok(task) => {
                worker_index += 1;
                senders[worker_index % num_threads].send(task).unwrap();
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    eprintln!("Finish reading verification task from lucene");
                } else {
                    eprintln!("Failed to read query: {:?}", e);
                }
                break;
            }
        }
    }
    for senders in senders {
        drop(senders);
    }
    for thread in threads {
        thread.join().unwrap();
    }
    eprintln!("Verified {} queries", verified.load(Ordering::SeqCst));
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        panic!("Missing required arguments");
    }
    verify_against_lucene(args[1].clone(), args[2].clone()).unwrap();
}
