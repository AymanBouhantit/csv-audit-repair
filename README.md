# csv-audit-repair

A small Rust library for opt-in repairs to imperfect CSV streams. It wraps any
`std::io::Read` and can feed an existing CSV parser.

The first repair removes spaces or tabs immediately before an opening quote at
the start of a field. For example, `name, "value, with comma"` becomes
`name,"value, with comma"`. Every removal produces a `Repair` with input byte
offset, record, field, and the exact bytes removed.

```rust
use csv_audit_repair::RepairReader;
use std::io::Read;

let input = b"name, \"value, with comma\"\n";
let mut reader = RepairReader::new(&input[..]);
let mut repaired = String::new();
reader.read_to_string(&mut repaired)?;
assert_eq!(repaired, "name,\"value, with comma\"\n");
assert_eq!(reader.take_repairs().len(), 1);
# Ok::<(), std::io::Error>(())
```

To use the [`csv` crate](https://docs.rs/csv/latest/csv/), pass the adapter to
`csv::ReaderBuilder::from_reader`:

```rust,ignore
let adapter = csv_audit_repair::RepairReader::new(file);
let mut parser = csv::ReaderBuilder::new().from_reader(adapter);
for row in parser.records() {
    process(row?);
}
let repairs = parser.into_inner().take_repairs();
```

The adapter uses comma delimiters and double-quote quoting. It does not
validate the full CSV grammar, fix unbalanced quotes, or infer missing
delimiters. Match the downstream parser's delimiter and quote settings to
these assumptions.

`Options::max_record_bytes` defaults to 8 MiB and applies to input bytes per
record, including quoted newlines. Audit events accumulate until drained with
`take_repairs`; drain regularly for long streams. If reading fails, earlier
output may be incomplete. This prototype copies bytes into an output buffer;
no zero-copy or performance claim is made yet.

Run `cargo test` and `cargo fmt --check` during development.

In local validation, public Shopify and WooCommerce samples passed through
unchanged, and a malformed CSV excerpt from
[rust-csv discussion #327](https://github.com/BurntSushi/rust-csv/discussions/327)
exercised the repair rule. No full malformed vendor export has yet been
validated.

Licensed under MIT.
