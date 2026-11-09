use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Value {
    Integer(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    pub offset: usize,
    pub message: &'static str,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bencode at byte {}: {}", self.offset, self.message)
    }
}

impl std::error::Error for Error {}

pub fn decode(input: &[u8]) -> Result<Value, Error> {
    let mut decoder = Decoder::new(input);
    let value = decoder.value(0)?;
    decoder.finish()?;
    Ok(value)
}

pub fn decode_root_dict_with_spans(
    input: &[u8],
) -> Result<(BTreeMap<Vec<u8>, Value>, BTreeMap<Vec<u8>, Range<usize>>), Error> {
    let mut decoder = Decoder::new(input);
    decoder.expect(b'd')?;
    let mut fields = BTreeMap::new();
    let mut spans = BTreeMap::new();
    let mut previous: Option<Vec<u8>> = None;

    while decoder.peek()? != b'e' {
        let key = decoder.bytes()?;
        decoder.check_order(previous.as_deref(), &key)?;
        previous = Some(key.clone());
        let start = decoder.offset;
        let value = decoder.value(1)?;
        spans.insert(key.clone(), start..decoder.offset);
        fields.insert(key, value);
    }

    decoder.expect(b'e')?;
    decoder.finish()?;
    Ok((fields, spans))
}

pub fn encode(value: &Value) -> Vec<u8> {
    let mut output = Vec::new();
    encode_into(value, &mut output);
    output
}

fn encode_into(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Integer(number) => {
            output.push(b'i');
            output.extend_from_slice(number.to_string().as_bytes());
            output.push(b'e');
        }
        Value::Bytes(bytes) => encode_bytes(bytes, output),
        Value::List(items) => {
            output.push(b'l');
            for item in items {
                encode_into(item, output);
            }
            output.push(b'e');
        }
        Value::Dict(fields) => {
            output.push(b'd');
            for (key, value) in fields {
                encode_bytes(key, output);
                encode_into(value, output);
            }
            output.push(b'e');
        }
    }
}

fn encode_bytes(bytes: &[u8], output: &mut Vec<u8>) {
    output.extend_from_slice(bytes.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(bytes);
}

struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn fail(&self, message: &'static str) -> Error {
        Error {
            offset: self.offset,
            message,
        }
    }

    fn peek(&self) -> Result<u8, Error> {
        self.input
            .get(self.offset)
            .copied()
            .ok_or_else(|| self.fail("unexpected end of input"))
    }

    fn expect(&mut self, expected: u8) -> Result<(), Error> {
        if self.peek()? != expected {
            return Err(self.fail("unexpected token"));
        }
        self.offset += 1;
        Ok(())
    }

    fn finish(&self) -> Result<(), Error> {
        if self.offset != self.input.len() {
            return Err(self.fail("trailing bytes"));
        }
        Ok(())
    }

    fn check_order(&self, previous: Option<&[u8]>, key: &[u8]) -> Result<(), Error> {
        if previous.is_some_and(|last| last >= key) {
            return Err(self.fail("dictionary keys are duplicate or out of order"));
        }
        Ok(())
    }

    fn bytes(&mut self) -> Result<Vec<u8>, Error> {
        let start = self.offset;
        while self.peek()?.is_ascii_digit() {
            self.offset += 1;
        }
        if self.offset == start || self.peek()? != b':' {
            return Err(self.fail("invalid byte-string length"));
        }
        let digits = &self.input[start..self.offset];
        if digits.len() > 1 && digits[0] == b'0' {
            return Err(self.fail("byte-string length has a leading zero"));
        }
        let mut length = 0_usize;
        for digit in digits {
            length = length
                .checked_mul(10)
                .and_then(|n| n.checked_add((digit - b'0') as usize))
                .ok_or_else(|| self.fail("byte-string length overflow"))?;
        }
        self.offset += 1;
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| self.fail("byte-string length overflow"))?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or_else(|| self.fail("truncated byte string"))?
            .to_vec();
        self.offset = end;
        Ok(bytes)
    }

    fn integer(&mut self) -> Result<i64, Error> {
        self.expect(b'i')?;
        let start = self.offset;
        while self.peek()? != b'e' {
            self.offset += 1;
        }
        let digits = &self.input[start..self.offset];
        let valid = !digits.is_empty()
            && digits != b"-0"
            && !(digits.len() > 1 && digits[0] == b'0')
            && !(digits.len() > 2 && digits.starts_with(b"-0"))
            && digits
                .iter()
                .enumerate()
                .all(|(i, b)| b.is_ascii_digit() || (i == 0 && *b == b'-'));
        if !valid || digits == b"-" {
            return Err(self.fail("invalid integer encoding"));
        }
        let number = std::str::from_utf8(digits)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| self.fail("integer out of range"))?;
        self.offset += 1;
        Ok(number)
    }

    fn value(&mut self, depth: usize) -> Result<Value, Error> {
        if depth > 64 {
            return Err(self.fail("maximum nesting depth exceeded"));
        }
        match self.peek()? {
            b'i' => self.integer().map(Value::Integer),
            b'l' => {
                self.offset += 1;
                let mut items = Vec::new();
                while self.peek()? != b'e' {
                    items.push(self.value(depth + 1)?);
                }
                self.offset += 1;
                Ok(Value::List(items))
            }
            b'd' => {
                self.offset += 1;
                let mut fields = BTreeMap::new();
                let mut previous: Option<Vec<u8>> = None;
                while self.peek()? != b'e' {
                    let key = self.bytes()?;
                    self.check_order(previous.as_deref(), &key)?;
                    previous = Some(key.clone());
                    fields.insert(key, self.value(depth + 1)?);
                }
                self.offset += 1;
                Ok(Value::Dict(fields))
            }
            b'0'..=b'9' => self.bytes().map(Value::Bytes),
            _ => Err(self.fail("invalid value token")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_nested_values_and_binary_bytes() {
        let value = Value::Dict(BTreeMap::from([
            (b"blob".to_vec(), Value::Bytes(vec![0, 255, 1, 0])),
            (b"empty".to_vec(), Value::List(vec![])),
            (
                b"nested".to_vec(),
                Value::List(vec![
                    Value::Integer(-42),
                    Value::Dict(BTreeMap::from([(
                        b"key".to_vec(),
                        Value::Bytes(b"value".to_vec()),
                    )])),
                ]),
            ),
        ]));
        assert_eq!(decode(&encode(&value)).unwrap(), value);
    }

    #[test]
    fn canonical_encoding_and_known_vectors() {
        assert_eq!(encode(&Value::Integer(0)), b"i0e");
        assert_eq!(encode(&Value::Bytes(b"spam".to_vec())), b"4:spam");
        assert_eq!(decode(b"l4:spami-3ee").unwrap(), Value::List(vec![
            Value::Bytes(b"spam".to_vec()),
            Value::Integer(-3),
        ]));
    }

    #[test]
    fn rejects_invalid_encodings() {
        for input in [
            &b"i-0e"[..],
            b"i03e",
            b"i-e",
            b"i9223372036854775808e",
            b"03:abc",
            b"4:abc",
            b"d1:bi1e1:ai2ee",
            b"d1:ai1e1:ai2ee",
            b"i1ei2e",
            b"l1:a",
        ] {
            assert!(decode(input).is_err(), "accepted {:?}", input);
        }
    }

    #[test]
    fn reports_exact_dictionary_value_spans() {
        let input = b"d4:infod1:xi4ee4:name4:teste";
        let (root, spans) = decode_root_dict_with_spans(input).unwrap();
        assert!(root.contains_key(&b"info"[..]));
        assert_eq!(&input[spans.get(&b"info"[..]).unwrap().clone()], b"d1:xi4ee");
    }
}
