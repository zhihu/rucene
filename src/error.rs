extern crate error_chain;
extern crate serde_json;

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
    }
}

impl<Guard> From<PoisonError<Guard>> for Error {
    fn from(_: PoisonError<Guard>) -> Error {
        ErrorKind::Poisoned.into()
    }
}
