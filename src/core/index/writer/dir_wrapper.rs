use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use core::store::directory::{
    Directory, FilterDirectory, LockValidatingDirectoryWrapper, TrackingDirectoryWrapper,
};
use core::store::io::{IndexInput, IndexOutput, RateLimitIndexOutput};
use core::store::{IOContext, RateLimiter};

use error::{ErrorKind::IllegalState, Result};
use thread_local::ThreadLocal;

pub type TrackingTmpDirectory<D> = TrackingTmpOutputDirectoryWrapper<
    TrackingDirectoryWrapper<
        LockValidatingDirectoryWrapper<D>,
        Arc<LockValidatingDirectoryWrapper<D>>,
    >,
>;

pub struct TrackingTmpOutputDirectoryWrapper<D: Directory> {
    directory: Arc<D>,
    pub file_names: Mutex<HashMap<String, String>>,
}

impl<D: Directory> TrackingTmpOutputDirectoryWrapper<D> {
    pub fn new(directory: Arc<D>) -> Self {
        Self {
            directory,
            file_names: Mutex::new(HashMap::new()),
        }
    }

    pub fn delete_temp_files(&self) {
        for file_name in self.file_names.lock().unwrap().values() {
            if let Err(e) = self.delete_file(file_name.as_str()) {
                warn!("delete file '{}' failed by '{:?}'", file_name, e);
            }
        }
    }
}

impl<D: Directory> FilterDirectory for TrackingTmpOutputDirectoryWrapper<D> {
    type Dir = D;

    fn dir(&self) -> &Self::Dir {
        &*self.directory
    }
}

impl<D: Directory> Directory for TrackingTmpOutputDirectoryWrapper<D> {
    type IndexOutput = D::TempOutput;
    type TempOutput = D::TempOutput;
    fn create_output(&self, name: &str, ctx: &IOContext) -> Result<Self::IndexOutput> {
        let mut guard = self.file_names.lock().unwrap();
        if guard.contains_key(name) {
            bail!(IllegalState(format!(
                "output file '{}' already exist!",
                name
            )));
        }

        self.dir().create_temp_output(name, "", ctx).map(|out| {
            guard.insert(name.to_string(), out.name().to_string());
            out
        })
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<dyn IndexInput>> {
        if let Some(n) = self.file_names.lock().unwrap().get(name) {
            self.dir().open_input(n, ctx)
        } else {
            bail!(IllegalState(format!("input file '{}' not found!", name)));
        }
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Self::TempOutput> {
        self.directory.create_temp_output(prefix, suffix, ctx)
    }
}

impl<D: Directory> fmt::Display for TrackingTmpOutputDirectoryWrapper<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TrackingTmpOutputDirectoryWrapper({})",
            self.directory.as_ref()
        )
    }
}

pub struct RateLimitFilterDirectory<D: Directory, RL: RateLimiter + ?Sized> {
    dir: Arc<D>,
    // reference to IndexWriter.rate_limiter
    rate_limiter: Arc<ThreadLocal<Arc<RL>>>,
}

impl<D, RL> RateLimitFilterDirectory<D, RL>
where
    D: Directory,
    RL: RateLimiter + ?Sized,
{
    pub fn new(dir: Arc<D>, rate_limiter: Arc<ThreadLocal<Arc<RL>>>) -> Self {
        RateLimitFilterDirectory { dir, rate_limiter }
    }
}

impl<D, RL> FilterDirectory for RateLimitFilterDirectory<D, RL>
where
    D: Directory,
    RL: RateLimiter + ?Sized,
{
    type Dir = D;

    #[inline]
    fn dir(&self) -> &Self::Dir {
        &*self.dir
    }
}

impl<D, RL> Directory for RateLimitFilterDirectory<D, RL>
where
    D: Directory,
    RL: RateLimiter + ?Sized,
{
    type IndexOutput = RateLimitIndexOutput<D::IndexOutput, RL>;
    type TempOutput = D::TempOutput;

    fn create_output(&self, name: &str, context: &IOContext) -> Result<Self::IndexOutput> {
        debug_assert!(context.is_merge());
        let rate_limiter = Arc::clone(&self.rate_limiter.get().unwrap());
        let index_output = self.dir.create_output(name, context)?;

        Ok(RateLimitIndexOutput::new(rate_limiter, index_output))
    }

    fn open_input(&self, name: &str, ctx: &IOContext) -> Result<Box<dyn IndexInput>> {
        self.dir.open_input(name, ctx)
    }

    fn create_temp_output(
        &self,
        prefix: &str,
        suffix: &str,
        ctx: &IOContext,
    ) -> Result<Self::TempOutput> {
        self.dir.create_temp_output(prefix, suffix, ctx)
    }
}

impl<D, RL> Clone for RateLimitFilterDirectory<D, RL>
where
    D: Directory,
    RL: RateLimiter + ?Sized,
{
    fn clone(&self) -> Self {
        RateLimitFilterDirectory {
            dir: Arc::clone(&self.dir),
            rate_limiter: Arc::clone(&self.rate_limiter),
        }
    }
}

impl<D, RL> fmt::Display for RateLimitFilterDirectory<D, RL>
where
    D: Directory,
    RL: RateLimiter + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RateLimitFilterDirectory({})", self.dir.as_ref())
    }
}
