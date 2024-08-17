use anyhow::Result;
use bytes::BytesMut;
use pack::{Pack, PackError};
use std::{borrow::Cow, cell::RefCell};

#[macro_export]
macro_rules! try_continue {
    ($msg:expr, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                log::error!("{}: {:?}", $msg, e);
                continue;
            }
        }
    };
}

#[macro_export]
macro_rules! try_break {
    ($lbl:tt, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                break $lbl Err(Error::from(e));
            }
        }
    };
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                break Err(Error::from(e));
            }
        }
    };
}

#[macro_export]
macro_rules! atomic_id {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct $name(u64);

        impl $name {
            pub fn new() -> Self {
                use std::sync::atomic::{AtomicU64, Ordering};
                static NEXT: AtomicU64 = AtomicU64::new(0);
                $name(NEXT.fetch_add(1, Ordering::Relaxed))
            }

            pub fn inner(&self) -> u64 {
                self.0
            }

            #[cfg(test)]
            #[allow(dead_code)]
            pub fn mk(i: u64) -> Self {
                $name(i)
            }
        }

        impl pack::Pack for $name {
            fn encoded_len(&self) -> usize {
                pack::varint_len(self.0)
            }

            fn encode(
                &self,
                buf: &mut impl bytes::BufMut,
            ) -> std::result::Result<(), pack::PackError> {
                Ok(pack::encode_varint(self.0, buf))
            }

            fn decode(
                buf: &mut impl bytes::Buf,
            ) -> std::result::Result<Self, pack::PackError> {
                Ok(Self(pack::decode_varint(buf)?))
            }
        }
    };
}

thread_local! {
    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(512));
}

/// pack-derive T and return a bytesmut from the global thread local buffer
pub fn pack<T: Pack>(t: &T) -> Result<BytesMut, PackError> {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        t.encode(&mut *b)?;
        Ok(b.split())
    })
}

pub fn is_sep(esc: &mut bool, c: char, escape: char, sep: char) -> bool {
    if c == sep {
        !*esc
    } else {
        *esc = c == escape && !*esc;
        false
    }
}

/// escape the specified string using the specified escape character
/// and a slice of special characters that need escaping.
pub fn escape<'a, 'b, T>(s: &'a T, esc: char, spec: &'b [char]) -> Cow<'a, str>
where
    T: AsRef<str> + ?Sized,
    'a: 'b,
{
    let s = s.as_ref();
    if s.find(|c: char| spec.contains(&c) || c == esc).is_none() {
        Cow::Borrowed(s.as_ref())
    } else {
        let mut out = String::with_capacity(s.len());
        for c in s.chars() {
            if spec.contains(&c) {
                out.push(esc);
                out.push(c);
            } else if c == esc {
                out.push(esc);
                out.push(c);
            } else {
                out.push(c);
            }
        }
        Cow::Owned(out)
    }
}

/// unescape the specified string using the specified escape character
pub fn unescape<T>(s: &T, esc: char) -> Cow<str>
where
    T: AsRef<str> + ?Sized,
{
    let s = s.as_ref();
    if !s.contains(esc) {
        Cow::Borrowed(s.as_ref())
    } else {
        let mut res = String::with_capacity(s.len());
        let mut escaped = false;
        res.extend(s.chars().filter_map(|c| {
            if c == esc && !escaped {
                escaped = true;
                None
            } else {
                escaped = false;
                Some(c)
            }
        }));
        Cow::Owned(res)
    }
}

pub fn is_escaped(s: &str, esc: char, i: usize) -> bool {
    let b = s.as_bytes();
    !s.is_char_boundary(i) || {
        let mut res = false;
        for j in (0..i).rev() {
            if s.is_char_boundary(j) && b[j] == (esc as u8) {
                res = !res;
            } else {
                break;
            }
        }
        res
    }
}

pub fn splitn_escaped(
    s: &str,
    n: usize,
    escape: char,
    sep: char,
) -> impl Iterator<Item = &str> {
    s.splitn(n, {
        let mut esc = false;
        move |c| is_sep(&mut esc, c, escape, sep)
    })
}

pub fn split_escaped(s: &str, escape: char, sep: char) -> impl Iterator<Item = &str> {
    s.split({
        let mut esc = false;
        move |c| is_sep(&mut esc, c, escape, sep)
    })
}

pub fn rsplit_escaped(s: &str, escape: char, sep: char) -> impl Iterator<Item = &str> {
    s.rsplit({
        let mut esc = false;
        move |c| is_sep(&mut esc, c, escape, sep)
    })
}

pub enum Either<T, U> {
    Left(T),
    Right(U),
}

impl<I, T: Iterator<Item = I>, U: Iterator<Item = I>> Iterator for Either<T, U> {
    type Item = I;

    fn next(&mut self) -> Option<I> {
        match self {
            Either::Left(t) => t.next(),
            Either::Right(t) => t.next(),
        }
    }
}
