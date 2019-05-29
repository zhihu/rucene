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

extern crate error_chain;
extern crate serde_json;

use core::index;
use core::search;
use core::search::collector;

use std::borrow::Cow;
use std::sync::PoisonError;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }
    errors {
        Poisoned {
            description("a thread holding the locked panicked and poisoned the lock")
        }

        IllegalState(desc: String) {
            description(desc)
            display("Illegal state: {}", desc)
        }

        IllegalArgument(desc: String) {
            description(desc)
            display("Illegal argument: {}", desc)
        }

        UnexpectedEOF(errmsg: String) {
            description(errmsg)
            display("Unexpected EOF: {}", errmsg)
        }

        CorruptIndex(errmsg: String) {
            description(errmsg)
            display("Corrupt Index: {}", errmsg)
        }

        UnsupportedOperation(errmsg: Cow<'static, str>) {
            description(errmsg),
            display("Unsupported Operation: {}", errmsg)
        }

        AlreadyClosed(errmsg: String) {
            description(errmsg)
            display("Already Closed: {}", errmsg)
        }

        IOError(errmsg: String) {
            description(errmsg)
            display("IO Error: {}", errmsg)
        }

        RuntimeError(errmsg: String) {
            description(errmsg)
            display("Runtime Error: {}", errmsg)
        }
    }

    foreign_links {
        FmtError(::std::fmt::Error);
        IoError(::std::io::Error);
        FromUtf8Err(::std::string::FromUtf8Error);
        Utf8Error(::std::str::Utf8Error);
        NumError(::std::num::ParseIntError);
        ParseFloatError(::std::num::ParseFloatError);
        SerdeJsonError(self::serde_json::Error);
        NulError(::std::ffi::NulError);
        TimeError(::std::time::SystemTimeError);
    }

    links {
        Collector(collector::Error, collector::ErrorKind);
        Search(search::Error, search::ErrorKind);
        Index(index::Error, index::ErrorKind);
    }
}

impl<Guard> From<PoisonError<Guard>> for Error {
    fn from(_: PoisonError<Guard>) -> Error {
        ErrorKind::Poisoned.into()
    }
}
