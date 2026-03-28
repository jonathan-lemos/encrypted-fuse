use assertables::assert_err;
use std::fmt::Debug;
use std::io::ErrorKind;

pub fn assert_error_kind<T: Debug>(result: std::io::Result<T>, kind: ErrorKind) {
    let err = assert_err!(&result);
    assert_eq!(err.kind(), kind);
}
