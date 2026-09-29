//! Streaming, auditable repair of whitespace before quoted CSV fields.
//!
//! ```
//! use csv_audit_repair::RepairReader;
//! use std::io::Read;
//!
//! let mut reader = RepairReader::new(&b"name, \"value, with comma\"\n"[..]);
//! let mut output = String::new();
//! reader.read_to_string(&mut output).unwrap();
//! assert_eq!(output, "name,\"value, with comma\"\n");
//! assert_eq!(reader.take_repairs()[0].removed, b" ");
//! ```
use std::io::{self, Read};

const CHUNK: usize = 8192;

/// An opt-in change made to the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repair {
    /// Byte offset of the first removed byte in the original input.
    pub input_offset: u64,
    /// Zero-based record number; quoted newlines do not end a record.
    pub record: u64,
    /// Zero-based field number.
    pub field: u64,
    /// Exact spaces or tabs removed before an opening quote.
    pub removed: Vec<u8>,
}

/// Input resource limits.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Maximum input bytes in one record, including the line ending.
    pub max_record_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_record_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy)]
enum State {
    Start,
    Unquoted,
    Quoted,
    AfterQuote,
}

/// A `Read` adapter that removes spaces or tabs immediately before a quoted
/// field and reports each removal. All other bytes pass through unchanged.
///
/// The adapter assumes comma delimiters and double-quote quoting. It does not
/// validate CSV grammar. Audit events are available after input processing,
/// which may precede delivery of buffered output bytes. Drain events regularly
/// for long streams. On error, previously returned output may be incomplete.
pub struct RepairReader<R> {
    inner: R,
    options: Options,
    input: [u8; CHUNK],
    input_pos: usize,
    input_len: usize,
    output: Vec<u8>,
    output_pos: usize,
    pending: Vec<u8>,
    pending_offset: u64,
    repairs: Vec<Repair>,
    state: State,
    offset: u64,
    record: u64,
    field: u64,
    record_bytes: usize,
    skip_lf_count: bool,
    finished: bool,
    failure: Option<(io::ErrorKind, String)>,
}

impl<R: Read> RepairReader<R> {
    /// Create an adapter with an 8 MiB record limit.
    pub fn new(inner: R) -> Self {
        Self::with_options(inner, Options::default())
    }

    /// Create an adapter with explicit limits.
    pub fn with_options(inner: R, options: Options) -> Self {
        Self {
            inner,
            options,
            input: [0; CHUNK],
            input_pos: 0,
            input_len: 0,
            output: Vec::with_capacity(CHUNK),
            output_pos: 0,
            pending: Vec::new(),
            pending_offset: 0,
            repairs: Vec::new(),
            state: State::Start,
            offset: 0,
            record: 0,
            field: 0,
            record_bytes: 0,
            skip_lf_count: false,
            finished: false,
            failure: None,
        }
    }

    /// Drain repair events accumulated so far.
    pub fn take_repairs(&mut self) -> Vec<Repair> {
        std::mem::take(&mut self.repairs)
    }

    /// Return the wrapped reader. Unread buffered bytes are discarded.
    pub fn into_inner(self) -> R {
        self.inner
    }

    fn flush_pending(&mut self) {
        self.output.extend_from_slice(&self.pending);
        self.pending.clear();
    }

    fn finish_record(&mut self, byte: u8) {
        self.output.push(byte);
        self.record += 1;
        self.field = 0;
        self.record_bytes = 0;
        self.state = State::Start;
        self.skip_lf_count = byte == b'\r';
    }

    fn process(&mut self, byte: u8, offset: u64) -> io::Result<()> {
        if self.skip_lf_count {
            self.skip_lf_count = false;
            if byte == b'\n' {
                self.output.push(byte);
                return Ok(());
            }
        }
        self.record_bytes = self.record_bytes.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "CSV record size overflow")
        })?;
        if self.record_bytes > self.options.max_record_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "CSV record {} exceeds {} input bytes at offset {}",
                    self.record, self.options.max_record_bytes, offset
                ),
            ));
        }
        match self.state {
            State::Start => match byte {
                b' ' | b'\t' => {
                    if self.pending.is_empty() {
                        self.pending_offset = offset;
                    }
                    self.pending.push(byte);
                }
                b'"' => {
                    if !self.pending.is_empty() {
                        self.repairs.push(Repair {
                            input_offset: self.pending_offset,
                            record: self.record,
                            field: self.field,
                            removed: std::mem::take(&mut self.pending),
                        });
                    }
                    self.output.push(byte);
                    self.state = State::Quoted;
                }
                b',' => {
                    self.flush_pending();
                    self.output.push(byte);
                    self.field += 1;
                }
                b'\r' | b'\n' => {
                    self.flush_pending();
                    self.finish_record(byte);
                }
                _ => {
                    self.flush_pending();
                    self.output.push(byte);
                    self.state = State::Unquoted;
                }
            },
            State::Unquoted => match byte {
                b',' => {
                    self.output.push(byte);
                    self.field += 1;
                    self.state = State::Start;
                }
                b'\r' | b'\n' => self.finish_record(byte),
                _ => self.output.push(byte),
            },
            State::Quoted => {
                self.output.push(byte);
                if byte == b'"' {
                    self.state = State::AfterQuote;
                }
            }
            State::AfterQuote => match byte {
                b'"' => {
                    self.output.push(byte);
                    self.state = State::Quoted;
                }
                b',' => {
                    self.output.push(byte);
                    self.field += 1;
                    self.state = State::Start;
                }
                b'\r' | b'\n' => self.finish_record(byte),
                _ => {
                    self.output.push(byte);
                    self.state = State::Unquoted;
                }
            },
        }
        Ok(())
    }

    fn fill_output(&mut self) {
        self.output.clear();
        self.output_pos = 0;
        while self.output.len() < CHUNK && !self.finished && self.failure.is_none() {
            if self.input_pos == self.input_len {
                match self.inner.read(&mut self.input) {
                    Ok(0) => {
                        self.flush_pending();
                        self.finished = true;
                        break;
                    }
                    Ok(n) => {
                        self.input_pos = 0;
                        self.input_len = n;
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        self.failure = Some((e.kind(), e.to_string()));
                        break;
                    }
                }
            }
            let byte = self.input[self.input_pos];
            self.input_pos += 1;
            let offset = self.offset;
            self.offset += 1;
            if let Err(e) = self.process(byte, offset) {
                self.failure = Some((e.kind(), e.to_string()));
            }
        }
    }
}

impl<R: Read> Read for RepairReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.output_pos == self.output.len() {
            self.fill_output();
        }
        let available = &self.output[self.output_pos..];
        if !available.is_empty() {
            let n = available.len().min(buf.len());
            buf[..n].copy_from_slice(&available[..n]);
            self.output_pos += n;
            return Ok(n);
        }
        if let Some((kind, message)) = &self.failure {
            return Err(io::Error::new(*kind, message.clone()));
        }
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(input: &[u8]) -> (Vec<u8>, Vec<Repair>) {
        let mut reader = RepairReader::new(input);
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        (output, reader.take_repairs())
    }

    #[test]
    fn repairs_spaces_before_opening_quote() {
        let (output, repairs) = run(b"a,  \"b,c\", plain,\t\"d\"\r\n");
        assert_eq!(output, b"a,\"b,c\", plain,\"d\"\r\n");
        assert_eq!(repairs.len(), 2);
        assert_eq!(
            (repairs[0].input_offset, repairs[0].record, repairs[0].field),
            (2, 0, 1)
        );
        assert_eq!(repairs[0].removed, b"  ");
        assert_eq!((repairs[1].record, repairs[1].field), (0, 3));
        assert_eq!(repairs[1].removed, b"\t");
    }

    #[test]
    fn preserves_quoted_newlines_escaped_quotes_and_unquoted_spaces() {
        let (output, repairs) = run(b"\"one\r\ntwo\"\"three\",  \"four\"\n  plain, tail  \n");
        assert_eq!(
            output,
            b"\"one\r\ntwo\"\"three\",\"four\"\n  plain, tail  \n"
        );
        assert_eq!((repairs[0].record, repairs[0].field), (0, 1));
        assert_eq!(repairs.len(), 1);
    }

    #[test]
    fn crosses_buffer_boundary_with_single_byte_reads() {
        let mut input = vec![b'x'; CHUNK - 1];
        input.extend_from_slice(b", \"yes\"");
        let mut reader = RepairReader::new(input.as_slice());
        let mut output = Vec::new();
        let mut byte = [0];
        while reader.read(&mut byte).unwrap() == 1 {
            output.push(byte[0]);
        }
        assert_eq!(&output[..CHUNK - 1], &input[..CHUNK - 1]);
        assert!(output.ends_with(b",\"yes\""));
        assert_eq!(reader.take_repairs()[0].input_offset, CHUNK as u64);
    }

    #[test]
    fn quoted_newline_does_not_reset_record_limit() {
        let mut reader = RepairReader::with_options(
            &b"\"a\nb\""[..],
            Options {
                max_record_bytes: 4,
            },
        );
        let mut output = Vec::new();
        assert_eq!(
            reader.read_to_end(&mut output).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn crlf_counts_as_one_record_end() {
        let mut reader = RepairReader::with_options(
            &b"a\r\nb\r\n"[..],
            Options {
                max_record_bytes: 2,
            },
        );
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        assert_eq!(output, b"a\r\nb\r\n");
    }

    #[test]
    fn trailing_whitespace_is_unchanged() {
        let (output, repairs) = run(b"a,  ");
        assert_eq!(output, b"a,  ");
        assert!(repairs.is_empty());
    }

    #[test]
    fn valid_utf8_csv_is_unchanged() {
        let input = "city,name\nMarrakech,\"أمين\"\n".as_bytes();
        let (output, repairs) = run(input);
        assert_eq!(output, input);
        assert!(repairs.is_empty());
    }
}
