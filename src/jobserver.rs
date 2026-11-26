use std::io;
use std::process::Command;
use std::sync::Arc;

use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::oneshot;

use crate::errors::*;

// The execution model of sccache is that on the first run it spawns a server
// in the background and detaches it.
// When normally executing the rust compiler from either cargo or make, it
// will use cargo/make's jobserver and limit its resource usage accordingly.
// When executing the rust compiler through the sccache server, that jobserver
// is not available, and spawning as many rustc as there are CPUs can lead to
// a quadratic use of the CPU resources (each rustc spawning as many threads
// as there are CPUs).
// One way around this issue is to inherit the jobserver from cargo or make
// when the sccache server is spawned, but that means that in some cases, the
// cargo or make process can't terminate until the sccache server terminates
// after its idle timeout (which also never happens if SCCACHE_IDLE_TIMEOUT=0).
// Also, if the sccache server ends up shared between multiple runs of
// cargo/make, then which jobserver is used doesn't make sense anymore.
// Ideally, the sccache client would give a handle to the jobserver it has
// access to, so that the rust compiler would "just" use the jobserver it
// would have used if it had run without sccache, but that adds some extra
// complexity, and requires to use Unix domain sockets.
// What we do instead is to arbitrary use our own jobserver.
// Unfortunately, that doesn't absolve us from having to deal with the original
// jobserver, because make may give us file descriptors to its pipes, and the
// simple fact of keeping them open can block it. That is handled by closing
// every inherited descriptor when the server detaches; see
// `util::close_inherited_fds`.

#[derive(Clone)]
pub struct Client {
    helper: Option<Arc<jobserver::HelperThread>>,
    tx: Option<mpsc::UnboundedSender<oneshot::Sender<io::Result<jobserver::Acquired>>>>,
    inner: jobserver::Client,
    fifo: bool,
    pool: Option<Arc<Pool>>,
}

/// Refills our private pool. Children we share it with (rustc) take tokens of
/// their own, and one killed mid-compile never writes them back; the pool lives
/// as long as the server, so the losses add up and the server runs ever fewer
/// compiles at once, down to one. When no token is held or awaited by us, every
/// token belongs in the pipe, so the shortfall there is exactly what was lost.
/// Tokens on their way to a waiting compile cannot be counted, so a busy server
/// stays short until its next idle moment.
struct Pool {
    client: jobserver::Client,
    capacity: usize,
    // Held or awaited tokens. Changed under the lock that also covers a refill,
    // so a refill never races a read for a new token.
    busy: std::sync::Mutex<usize>,
}

impl Pool {
    fn enter(self: &Arc<Self>) -> Acquired {
        *self.busy.lock().unwrap() += 1;
        Acquired {
            token: None,
            pool: Some(self.clone()),
        }
    }
}

/// Also the guard of [`Pool::busy`]: dropping it, acquired or not, leaves.
pub struct Acquired {
    token: Option<jobserver::Acquired>,
    pool: Option<Arc<Pool>>,
}

impl Drop for Acquired {
    fn drop(&mut self) {
        // The token goes back before the shortfall is measured.
        drop(self.token.take());
        let Some(pool) = &self.pool else { return };
        let mut busy = pool.busy.lock().unwrap();
        *busy -= 1;
        if *busy == 0 {
            // Both ways: a cancelled `acquire` whose read was already under way
            // puts its byte back after this count, leaving one too many.
            let available = pool.client.available().unwrap_or(pool.capacity);
            for _ in available..pool.capacity {
                drop(pool.client.release_raw());
            }
            for _ in pool.capacity..available {
                drop(pool.client.acquire_raw());
            }
        }
    }
}

impl Client {
    pub fn new() -> Client {
        Client::new_num(crate::util::num_cpus())
    }

    /// The jobserver of the build that ran us, if it is one opened by path
    /// (`--jobserver-auth=fifo:PATH`, GNU make 4.4 and later). The problems
    /// above are about inherited pipe descriptors; a path holds nothing of
    /// the build's open, so the client, which lives only as long as one
    /// compile, can use it and hand it on to what it runs. Whoever ran us
    /// already holds a token for this compile, so none is taken per spawn.
    pub fn from_env_fifo() -> Option<Client> {
        if !env_names_fifo() {
            return None;
        }
        // SAFETY: a `fifo:` jobserver is opened by path; no inherited file
        // descriptors are taken over.
        let inner = unsafe { jobserver::Client::from_env() }?;
        Some(Client::_new(inner, true))
    }

    pub fn new_num(num: usize) -> Client {
        let inner = jobserver::Client::new(num).expect("failed to create jobserver");
        let mut client = Client::_new(inner, false);
        client.pool = Some(Arc::new(Pool {
            client: client.inner.clone(),
            capacity: num,
            busy: Default::default(),
        }));
        client
    }

    fn _new(inner: jobserver::Client, fifo: bool) -> Client {
        let (helper, tx) = if fifo {
            (None, None)
        } else {
            let (tx, mut rx) = mpsc::unbounded::<oneshot::Sender<_>>();
            let helper = inner
                .clone()
                .into_helper_thread(move |token| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    rt.block_on(async {
                        if let Some(sender) = rx.next().await {
                            drop(sender.send(token));
                        }
                    });
                })
                .expect("failed to spawn helper thread");
            (Some(Arc::new(helper)), Some(tx))
        };

        Client {
            inner,
            helper,
            tx,
            fifo,
            pool: None,
        }
    }

    /// Whether this is the build's own jobserver, opened by path (see
    /// [`Client::from_env_fifo`]), rather than a private pool.
    pub fn is_fifo(&self) -> bool {
        self.fifo
    }

    /// Configures this jobserver to be inherited by the specified command
    pub fn configure(&self, cmd: &mut Command) {
        self.inner.configure(cmd);
    }

    /// Returns a future that represents an acquired jobserver token.
    ///
    /// This should be invoked before any "work" is spawned (for whatever the
    /// definition of "work" is) to ensure that the system is properly
    /// rate-limiting itself.
    pub async fn acquire(&self) -> Result<Acquired> {
        let (helper, tx) = match (self.helper.as_ref(), self.tx.as_ref()) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                return Ok(Acquired {
                    token: None,
                    pool: None,
                });
            }
        };
        // Entered before the read, so a refill never runs while it is under way.
        let mut guard = match &self.pool {
            Some(pool) => pool.enter(),
            None => Acquired {
                token: None,
                pool: None,
            },
        };
        let (mytx, myrx) = oneshot::channel();
        helper.request_token();
        tx.unbounded_send(mytx).unwrap();

        let acquired = myrx
            .await
            .context("jobserver helper panicked")?
            .context("failed to acquire jobserver token")?;

        guard.token = Some(acquired);
        Ok(guard)
    }
}

/// Whether the jobserver in our environment, as `jobserver::Client::from_env`
/// would pick it, is opened by path.
fn env_names_fifo() -> bool {
    ["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS"]
        .iter()
        .find_map(std::env::var_os)
        .is_some_and(|flags| flags.to_str().is_some_and(names_fifo))
}

fn names_fifo(flags: &str) -> bool {
    flags
        .split_ascii_whitespace()
        .any(|arg| arg.starts_with("--jobserver-auth=fifo:"))
}

#[cfg(test)]
mod test {
    use super::{Client, names_fifo};

    #[test]
    fn fifo_jobserver_flags() {
        assert!(names_fifo("-j8 --jobserver-auth=fifo:/tmp/GMfifo1234"));
        assert!(names_fifo(" -j --jobserver-auth=fifo:/dev/steve"));
        assert!(!names_fifo("-j8 --jobserver-fds=3,4 --jobserver-auth=3,4"));
        assert!(!names_fifo("-j8"));
        assert!(!names_fifo(""));
    }

    #[test]
    fn private_pool_is_not_fifo() {
        assert!(!Client::new_num(1).is_fifo());
    }

    /// rustc takes extra tokens from the pool we share with it, and a rustc
    /// killed mid-compile (the server kills one whose client went away) never
    /// writes them back. The pool lives as long as the server, so each such
    /// kill shrinks it.
    #[test]
    #[cfg(unix)]
    fn tokens_held_by_a_killed_child_return_to_the_pool() {
        use crate::mock_command::{
            CommandChild, CommandCreator, ProcessCommandCreator, RunCommand,
        };
        use tokio::io::AsyncReadExt;

        let client = Client::new_num(2);
        let mut creator = <ProcessCommandCreator as CommandCreator>::new(&client);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut cmd = creator.new_command("/bin/sh");
            cmd.args(&[
                "-c",
                "a=${CARGO_MAKEFLAGS##*--jobserver-auth=}; a=${a%% *}; \
                 head -c1 <&${a%%,*} >/dev/null && echo held && exec sleep 60",
            ])
            .stdout(std::process::Stdio::piped())
            .share_jobserver();
            let mut child = cmd.spawn().await.unwrap();
            let mut held = [0; 4];
            child
                .take_stdout()
                .unwrap()
                .read_exact(&mut held)
                .await
                .unwrap();
            drop(child);
        });
        assert_eq!(client.inner.available().unwrap(), 2);
    }
}
