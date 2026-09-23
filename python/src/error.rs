use hdfs_native::HdfsError as RustHdfsError;
use pyo3::{PyErr, create_exception, exceptions::*};

create_exception!(_internal, HdfsError, PyException);

create_exception!(_internal, DataTransferError, HdfsError);
create_exception!(_internal, ChecksumError, HdfsError);
create_exception!(_internal, InvalidPath, HdfsError);
create_exception!(_internal, InvalidArgument, HdfsError);
create_exception!(_internal, UrlParseError, HdfsError);
create_exception!(_internal, OperationFailed, HdfsError);
create_exception!(_internal, BlocksNotFound, HdfsError);
create_exception!(_internal, TrashNotEnabled, HdfsError);
create_exception!(_internal, UnsupportedErasureCodingPolicy, HdfsError);
create_exception!(_internal, ErasureCodingError, HdfsError);
create_exception!(_internal, InternalError, HdfsError);
create_exception!(_internal, InvalidRPCResponse, HdfsError);
create_exception!(_internal, RPCError, HdfsError);
create_exception!(_internal, FatalRPCError, HdfsError);
create_exception!(_internal, SASLError, HdfsError);
create_exception!(_internal, GSSAPIError, HdfsError);
create_exception!(_internal, NoSASLMechanism, HdfsError);
create_exception!(_internal, XmlParseError, HdfsError);

pub struct PythonHdfsError(RustHdfsError);

impl From<RustHdfsError> for PythonHdfsError {
    fn from(value: RustHdfsError) -> Self {
        PythonHdfsError(value)
    }
}

impl From<PythonHdfsError> for PyErr {
    fn from(value: PythonHdfsError) -> Self {
        let err = value.0;
        let message = err.to_string();
        match err {
            RustHdfsError::IOError(err) => PyIOError::new_err(err),
            RustHdfsError::AlreadyExists(path) => PyFileExistsError::new_err(path),
            RustHdfsError::FileNotFound(path) => PyFileNotFoundError::new_err(path),
            RustHdfsError::IsADirectoryError(path) => PyIsADirectoryError::new_err(path),
            RustHdfsError::UnsupportedFeature(feat) => PyNotImplementedError::new_err(feat),
            RustHdfsError::DataTransferError(_) => DataTransferError::new_err(message),
            RustHdfsError::ChecksumError => ChecksumError::new_err(message),
            RustHdfsError::InvalidPath(_) => InvalidPath::new_err(message),
            RustHdfsError::InvalidArgument(_) => InvalidArgument::new_err(message),
            RustHdfsError::UrlParseError(_) => UrlParseError::new_err(message),
            RustHdfsError::OperationFailed(_) => OperationFailed::new_err(message),
            RustHdfsError::BlocksNotFound(_) => BlocksNotFound::new_err(message),
            RustHdfsError::TrashNotEnabled => TrashNotEnabled::new_err(message),
            RustHdfsError::UnsupportedErasureCodingPolicy(_) => {
                UnsupportedErasureCodingPolicy::new_err(message)
            }
            RustHdfsError::ErasureCodingError(_) => ErasureCodingError::new_err(message),
            RustHdfsError::InternalError(_) => InternalError::new_err(message),
            RustHdfsError::InvalidRPCResponse(_) => InvalidRPCResponse::new_err(message),
            RustHdfsError::RPCError(_, _) => RPCError::new_err(message),
            RustHdfsError::FatalRPCError(_, _) => FatalRPCError::new_err(message),
            RustHdfsError::SASLError(_) => SASLError::new_err(message),
            RustHdfsError::GSSAPIError(_, _, _) => GSSAPIError::new_err(message),
            RustHdfsError::NoSASLMechanism => NoSASLMechanism::new_err(message),
            RustHdfsError::XmlParseError(_) => XmlParseError::new_err(message),
        }
    }
}
