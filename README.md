# hkvdb

[![Rust build status](https://img.shields.io/github/workflow/status/travisbrown/hkvdb/rust-ci.svg?label=rust)](https://github.com/travisbrown/hkvdb/actions)
[![Coverage status](https://img.shields.io/codecov/c/github/travisbrown/hkvdb/main.svg)](https://codecov.io/github/travisbrown/hkvdb)

Please note that this software is **not** "open source",
but the source is available for use and modification by individuals, non-profit organizations, and worker-owned cooperatives
(see the [license section](#license) below for details).

## About

This is a tiny project that packages up some code I was using in several places.
The motivation is that in several projects recently I've needed a way to store
historical observations associating some entity with a field value at a particular time.
For example, we might want to collect profile image URLs for a selection of Twitter
accounts, while keeping track of the first and last dates that each profile image
is known to have been used.

We could use a relational database for this kind of task, but this data is often just
an intermediate step in some other process, and the structure is very minimal, so
setting up a database in something like Postgres (or even SQLite) feels like overkill.
Using a [RocksDB][rocksdb] store gives us a lightweight way to work with this data
efficiently without much setup cost.

For example, it takes around 35 minutes to load profile image URLs from 180 million
user profile snapshots from a month of [Twitter Stream Grab][tsg] data from 2021,
and the resulting store (containing 38,937,009 URLs for 31,393,631 accounts) is only 2.2 GB.
Looking up data values is pretty fast, with very little startup time. For example,
searching for profile image URLs and dates of use for a thousand users only takes a few dozen
milliseconds on my machine:

```
$ time target/release/demo data/profile-image-demo-2021-08/ < users-ids-1k.txt > out.txt

real	0m0.053s
user	0m0.049s
sys     0m0.034s
```

The version of this implementation here currently only supports tracking observed date
ranges or instances (using epoch seconds). ~~It also only supports lookup by key (e.g.
Twitter user ID in the example above), although I have code for indexing values that
I'll integrate at some point~~ [I did a quick job of copying some of this over but it's
still untested].

## Usage

There's not much to the API beyond `put` and `get`. For example, suppose we have some data
like this:

```rust

struct UserSnapshot {
    user_id: u64,
    timestamp_s: u32,
    screen_name: String,
}

let snapshots = vec![
    UserSnapshot {
        user_id: 770781940341288960,
        timestamp_s: 1577933499,
        screen_name: "RudyGiuliani".to_string(),
    },
    UserSnapshot {
        user_id: 770781940341288960,
        timestamp_s: 1479920042,
        screen_name: "xxxxxxx37583982".to_string(),
    },
    UserSnapshot {
        user_id: 6510972,
        timestamp_s: 1643648042,
        screen_name: "travisbrown".to_string(),
    },
    // Millions of other user profile snapshots?
];
```

We can create a database and insert this user data with `put`:

```rust
use hkvdb::Hkvdb;

let db: Hkvdb<Range32> = Hkvdb::new("profile-image-urls")?;

for snapshot in snapshots {
    db.put(
        snapshot.user_id,
        &snapshot.screen_name,
        snapshot.timestamp_s,
    )?;
}
```

And look up all data values associated with an ID with `get`:

```rust
let values = db.get(770781940341288960)?;

let mut expected = HashMap::new();
expected.insert("xxxxxxx37583982".to_string(), 1479920042.into());
expected.insert("RudyGiuliani".to_string(), 1577933499.into());

assert_eq!(values, expected);
```

We can also optionally create an index and search the database by data value:

```rust
db.make_index(CaseSensitivity::Insensitive)?;

let user_ids = db.search_ci("RuDYgiuLianI")?;

assert_eq!(user_ids, vec![770781940341288960]);
```

That's about all you can do with it for now!

## Details

The format is very simple, and should be easily readable from any RocksDB client or library.
For example, for the version with date ranges:

```
+----------------------------+---------+
| key                        | value   |
+-+--------+---------········+----+----+
|0| id     | data            |1st |last|
+-+--------+---------········+----+----+
```

The version that stores all observed timestamps just has more stuff in the value part:

```
+----------------------------+--------------+
| key                        | value        |
+-+--------+---------········+----+----+····|
|0| id     | data            |1st |2nd |etc.|
+-+--------+---------········+----+----+····|
```

If you generate the index, the additional rows look like this:

```
+-------------------+--------------------------+
| key               | value                    |
+-+---------········+--------+--------+········|
|1| data            | id 1   | id 2   | etc.   |
+-+---------········+--------+--------+········|
```

All integers are stored using the big-endian byte ordering.

## License

This software is published under the [Anti-Capitalist Software License][acsl] (v. 1.4).

[acsl]: https://anticapitalist.software/
[rocksdb]: https://rocksdb.org
[tsg]: https://archive.org/details/twitterstream
