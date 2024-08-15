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
